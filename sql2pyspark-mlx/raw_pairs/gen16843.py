from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .cube(
        F.date_trunc("YEAR", "prev_started_at").alias("year_start"),
        F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL").alias("new_group_flag_tier"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "DE", F.col("new_group_flag")).otherwise(0)).alias("de_new_group_flag"),
        F.min("prev_started_at").alias("first_prev_started_at"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
    .filter(F.col("de_new_group_flag") > 0)
)
