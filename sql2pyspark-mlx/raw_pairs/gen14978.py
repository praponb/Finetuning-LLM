from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("amount", "id")
w3 = Window.partitionBy(F.month("paid_at")).orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("status", "order_id").orderBy(F.col("amount").asc_nulls_first())
w6 = Window.partitionBy("order_id").orderBy(F.col("amount_bucket_max_90d").asc_nulls_first())
w7 = Window.partitionBy("order_id")

metrics = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.max("amount").over(w).alias("last3_max_amount"),
        F.count("amount").over(w).alias("last3_count_amount"),
        F.sum("amount").over(w).alias("last3_sum_amount"),
        F.ntile(3).over(w2).alias("amount_bucket"),
    )
)

kept_keys = (
    metrics
    .select("status", "order_id")
    .intersect(
        df_payments
        .filter(F.col("order_id") == 20)
        .select("status", "order_id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

metrics_payments = (
    reconciled
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.max("amount_bucket").over(w3).alias("amount_bucket_max_90d"),
        F.count("amount").over(w4).alias("amount_count_30d"),
        F.rank().over(w5).alias("amount_rank"),
    )
)

joined = (
    metrics_payments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee") < 29)
    .select("a.*", F.col("o.shipping_fee"))
)

metrics_payments_2 = (
    joined
    .select(
        "order_id",
        "amount",
        "amount_bucket_max_90d",
        "amount_count_30d",
        F.percent_rank().over(w6).alias("amount_bucket_max_90d_percent_rank"),
        F.max("amount_count_30d").over(w7).alias("order_id_max_amount_count_30d"),
    )
)

result = (
    metrics_payments_2
    .groupBy(
        F.when(F.col("amount_bucket_max_90d") >= 8, "LARGE").otherwise("SMALL").alias("amount_bucket_max_90d_tier"),
        F.when(F.col("amount") >= 1350, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("amount_bucket_max_90d_percent_rank", 0.5).alias("amount_bucket_max_90d_percent_rank_p50"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.max("amount_bucket_max_90d_percent_rank").alias("max_amount_bucket_max_90d_percent_rank"),
    )
    .orderBy(F.col("amount_p50").desc())
)
