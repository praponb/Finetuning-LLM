from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "desktop") & (F.col("started_at") >= "2023-06-01"))
    .select(
        "id",
        "country",
        "device",
        "browser",
        F.date_trunc("year", "started_at").alias("started_at_period"),
        F.substring("country", 1, 3).alias("country_prefix"),
    )
)
