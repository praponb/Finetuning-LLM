from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-04-24 21:00:00").cast("timestamp")) & F.col("country").isNotNull() & F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)

result = (
    base
    .groupBy("device")
    .agg(
        F.count_if(F.col("browser") == "Safari").alias("safari_events"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.count("*").alias("num_rows"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.max("started_at").alias("last_started_at"),
    )
)
