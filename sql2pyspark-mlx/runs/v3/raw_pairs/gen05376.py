from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") == "US") & ((F.to_date("started_at") >= "2023-06-01") | (F.col("device") == "desktop")))
    .select(
        "id",
        "started_at",
        "device",
        "customer_id",
        F.concat_ws("/", "country", "browser").alias("country_browser_key"),
    )
)
