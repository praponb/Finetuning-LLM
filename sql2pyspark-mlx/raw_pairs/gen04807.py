from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (F.col("country") == "UK"))
    .select(
        "id",
        "started_at",
        "customer_id",
        F.concat_ws("-", "country", "browser").alias("country_browser_key"),
    )
)
