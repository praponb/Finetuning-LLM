from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.when(F.col("session_id") >= 7, "HIGH").otherwise("LOW").alias("session_id_tier"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "DE", F.col("new_group_flag"))).alias("de_total_new_group_flag"),
        F.percentile_approx("new_group_flag", 0.9).alias("new_group_flag_p90"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"),
    )
    .orderBy(F.col("new_group_flag_p90").desc_nulls_last())
)
