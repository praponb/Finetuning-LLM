from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .withColumn("week_start", F.date_trunc("WEEK", "paid_at"))
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .cube("week_start", "session_start_flag_tier")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.max("paid_at").alias("last_seen"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("week_start").desc_nulls_last())
)
