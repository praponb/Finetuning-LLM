from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

kept_keys = (
    df_payments
    .select("order_id", "status")
    .intersect(
        df_payments
        .filter(F.col("paid_at") < "2025-09-01")
        .select("order_id", "status")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("amount_cume_dist", F.cume_dist().over(w))
    .withColumn("amount_count_30d", F.count("amount").over(w2))
)

cleaned = (
    metrics
    .filter(F.col("amount_cume_dist") < 0.49)
    .select(
        "*",
        F.substring("status", 1, 2).alias("status_prefix"),
        F.round("amount_count_30d", 2).alias("amount_count_30d_rounded"),
    )
)

matched_keys = (
    cleaned
    .select("id", "order_id")
    .intersectAll(
        df_payments
        .filter(F.col("order_id").isin([9, 14, 31, 32]))
        .select("id", "order_id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("amount") >= 1230, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("amount_cume_dist", [0.25, 0.5, 0.75]).alias("amount_cume_dist_quartiles"),
        F.round(F.sum("amount_cume_dist") / F.nullif(F.count(F.when(F.col("method") == "paypal", 1)), F.lit(0)), 2).alias("amount_cume_dist_per_paypal"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").asc_nulls_last())
)
