from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "DE")
    .select(
        "id",
        "customer_id",
        F.concat_ws("-", "browser", "country").alias("browser_country_key"),
        F.substring("browser", 1, 3).alias("browser_prefix"),
    )
)
