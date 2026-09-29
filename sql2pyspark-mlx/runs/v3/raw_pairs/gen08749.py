from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "country",
        "customer_id",
        F.concat("device", F.lit(", "), "browser").alias("device_browser"),
    )
)
