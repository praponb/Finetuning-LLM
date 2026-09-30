from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"))
)

result = (
    latest_match
    .cube("browser", "country", "customer_id")
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.countDistinct("country").alias("distinct_countries"),
        F.grouping("browser").alias("gid"),
    )
)
