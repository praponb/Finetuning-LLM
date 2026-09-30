from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .rollup(F.date_trunc("week", "started_at").alias("week_start"), "device")
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.max("started_at").alias("last_started_at"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("browser") == "Safari", 1)), F.lit(0)), 2).alias("is_new_burst_per_safari"),
    )
    .orderBy(F.col("avg_island_id").asc_nulls_last())
)
