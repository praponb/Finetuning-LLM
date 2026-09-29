from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
        F.min("started_at").alias("first_seen"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("country")).alias("num_countries"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.max("started_at").alias("last_started_at"),
    )
    .orderBy(F.col("num_countries").desc())
)
