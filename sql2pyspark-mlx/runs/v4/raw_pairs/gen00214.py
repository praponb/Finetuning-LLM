from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id", "method").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("cumulative_burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("status")

flagged = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

with_windows = (
    sessionized
    .withColumn("burst_id_sum_7d", F.sum("burst_id").over(w3))
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w4))
    .withColumn("cumulative_burst_id", F.sum("burst_id").over(w5))
)

scored = (
    with_windows
    .withColumn("cumulative_burst_id_sum_7d", F.sum("burst_id_sum_7d").over(w6))
    .withColumn("status_min_cumulative_burst_id", F.min("cumulative_burst_id").over(w_recent))
    .withColumn("status_sum_new_group_flag", F.sum("new_group_flag").over(w_recent))
)

summary = (
    scored
    .groupBy(
        F.when(F.col("status_sum_new_group_flag") >= 3, "PREMIUM").otherwise("STANDARD").alias("status_sum_new_group_flag_tier"),
    )
    .agg(
        F.percentile_approx("burst_id", 0.9).alias("burst_id_p90"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    summary
    .select("status_sum_new_group_flag_tier")
    .orderBy("status_sum_new_group_flag_tier")
)
