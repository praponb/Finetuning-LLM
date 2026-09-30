from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat("browser", F.lit(" "), "country").alias("browser_country"),
        F.length("device").alias("device_length"),
    )
)

result = (
    valid
    .groupBy("device", "country")
    .agg(
        F.max("started_at").alias("last_seen"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.round(F.sum("device_length") / F.nullif(F.count(F.when(F.col("device") == "mobile", 1)), F.lit(0)), 2).alias("device_length_per_mobile"),
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
    )
    .filter(F.col("uk_rows") < 1)
    .orderBy(F.col("de_rows").asc_nulls_last())
)
