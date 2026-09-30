from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("burst_id").orderBy(F.col("amount_stddev").desc())

flagged = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .groupBy("burst_id", F.date_trunc("MONTH", "paid_at").alias("activity_month"))
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .withColumn("amount_stddev_percent_rank", F.percent_rank().over(w3))
)
