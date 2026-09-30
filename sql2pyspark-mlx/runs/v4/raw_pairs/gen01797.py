from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-05-18").cast("date"))
    .select(
        "*",
        F.trim("device").alias("clean_device"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_clean"),
    )
)

result = (
    filtered
    .groupBy(F.date_trunc("year", "started_at").alias("year_start"))
    .agg(
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.max("started_at").alias("last_seen"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)
