from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        "customer_id",
        F.when(F.col("burst_id") >= 9, "HIGH").otherwise("LOW").alias("burst_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
    )
    .orderBy(F.col("customer_id").asc_nulls_last())
)
