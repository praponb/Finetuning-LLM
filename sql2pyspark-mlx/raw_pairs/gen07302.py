from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("WEEK", "paid_at").alias("week_start"), "status")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
)
