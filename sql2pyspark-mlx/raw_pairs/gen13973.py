from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_payments
    .groupBy("method")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.max("paid_at").alias("ended_at"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_rows"),
    )
    .filter(F.col("distinct_order_ids") < 1000)
    .orderBy(F.col("distinct_order_ids").desc())
)
