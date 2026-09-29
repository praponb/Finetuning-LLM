from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([7, 12, 19]))
    .select(
        "id",
        "customer_id",
        "country",
        "browser",
        F.concat_ws("-", "country", "browser").alias("country_browser_key"),
    )
)
