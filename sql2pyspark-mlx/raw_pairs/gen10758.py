from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("island_id", "id")
w4 = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at").desc_nulls_last())
w5 = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").desc_nulls_last(), "id")

gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

ranked = (
    sessionized
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest = (
    ranked
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

ranked_payments = (
    latest
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    ranked_payments
    .groupBy(
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
        F.when(F.col("island_id") >= 8, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.max(F.when(F.col("status") == "SUCCESS", F.col("island_id"))).alias("success_max_island_id"),
    )
    .orderBy(F.col("success_max_island_id").desc())
)
