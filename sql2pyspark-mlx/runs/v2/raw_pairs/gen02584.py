from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.to_date("started_at") < "2024-08-01") | F.col("customer_id").isin([29, 40]))
    .select(
        "customer_id",
        "country",
        F.upper("browser").alias("browser_upper"),
        F.concat("device", F.lit("/"), "country").alias("device_country"),
    )
)
