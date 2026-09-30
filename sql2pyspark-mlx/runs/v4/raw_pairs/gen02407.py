from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_last())

matched_keys = (
    df_payments
    .select("id", "order_id")
    .intersect(
        df_payments
        .filter(F.col("status") == "REFUNDED")
        .select("id", "order_id")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("amount_cume_dist", F.cume_dist().over(w))
    .withColumn("amount_sum_7d", F.sum("amount").over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
    .withColumn("amount_cume_dist_2", F.cume_dist().over(w4))
    .withColumn("amount_sum_7d_2", F.sum("amount").over(w2))
)

base = (
    with_windows
    .filter(F.col("amount_cume_dist_2") > 0.75)
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "amount_cume_dist",
        "amount_sum_7d",
        "amount_share",
        "amount_cume_dist_2",
        "amount_sum_7d_2",
        F.round("amount_sum_7d_2", 1).alias("amount_sum_7d_2_rounded"),
        (F.col("amount_cume_dist_2") + F.col("amount_sum_7d")).alias("amount_cume_dist_2_plus_amount_sum_7d"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

summary = (
    base
    .groupBy(
        F.when(F.col("amount_sum_7d") >= 7042, "HIGH").otherwise("LOW").alias("amount_sum_7d_tier"),
        "order_id",
    )
    .agg(
        F.count_if(F.col("status") == "SUCCESS").alias("success_events"),
        F.count("*").alias("event_count"),
        F.countDistinct("status").alias("distinct_status"),
    )
)

result = (
    summary
    .select("amount_sum_7d_tier", "order_id", "event_count", "distinct_status")
    .orderBy(F.col("distinct_status").asc_nulls_first())
)
