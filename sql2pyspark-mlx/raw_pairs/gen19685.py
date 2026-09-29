from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .withColumn("is_new_burst_tier", F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .rollup("is_new_burst_tier", "year_start")
    .agg(
        F.max(F.when(F.col("status") == "SUCCESS", F.col("island_id"))).alias("success_max_island_id"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.max(F.when(F.col("method") == "cash", F.col("is_new_burst"))).alias("cash_max_is_new_burst"),
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
        F.grouping_id().alias("grouping_id"),
    )
)
