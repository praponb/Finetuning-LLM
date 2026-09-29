from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("order_id") == 29)
    .select(
        "*",
        F.concat_ws("/", "status", "method").alias("status_method_key"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

gaps = (
    base
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("activity_month"))
    .agg(
        F.min("paid_at").alias("started_at"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.avg("new_group_flag"), 2).alias("avg_new_group_flag"),
    )
    .orderBy("avg_new_group_flag")
)
