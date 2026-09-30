from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("method", "order_id")
w3 = Window.partitionBy("status").orderBy("amount_avg_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("paid_at"), "order_id").orderBy("method_min_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("activity_month").orderBy(F.col("min_running_count_amount_avg_90d").desc())

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-09-23 08:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat("status", F.lit(" "), "method").alias("status_method"),
        F.trim("method").alias("clean_method"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "status_method",
        F.avg("amount").over(w).alias("amount_avg_90d"),
        F.min("amount").over(w2).alias("method_min_amount"),
    )
)

enriched = (
    scored
    .filter(F.col("method_min_amount").between(1262, 1549))
    .select(
        "id",
        "paid_at",
        "status",
        "method_min_amount",
        F.avg("amount_avg_90d").over(w3).alias("running_avg_amount_avg_90d"),
        F.sum("amount_avg_90d").over(w3).alias("running_sum_amount_avg_90d"),
        F.min("amount_avg_90d").over(w3).alias("running_min_amount_avg_90d"),
        F.count("amount_avg_90d").over(w3).alias("running_count_amount_avg_90d"),
        F.sum("method_min_amount").over(w4).alias("running_method_min_amount"),
    )
)

cleaned = (
    enriched
    .filter(F.col("running_avg_amount_avg_90d") >= 680)
    .select(
        "id",
        "paid_at",
        "status",
        "method_min_amount",
        "running_avg_amount_avg_90d",
        "running_sum_amount_avg_90d",
        "running_count_amount_avg_90d",
        "running_method_min_amount",
        F.upper("status").alias("status_upper"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
    )
    .agg(
        F.max("paid_at").alias("last_seen"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.min("running_count_amount_avg_90d").alias("min_running_count_amount_avg_90d"),
    )
    .withColumn("min_running_count_amount_avg_90d_dense_rank", F.dense_rank().over(w5))
)
