from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select("*", F.lower("browser").alias("browser_lower"))
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("session_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("session_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
    )
    .filter(F.col("safari_count") >= 1)
    .orderBy(F.col("session_id_tier").desc())
)
