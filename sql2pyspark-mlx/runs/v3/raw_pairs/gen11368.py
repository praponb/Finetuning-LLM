from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 4)
    .select(
        "id",
        "customer_id",
        "started_at",
        F.concat_ws("-", "browser", "country").alias("browser_country_key"),
    )
    .orderBy("id", "started_at")
)
