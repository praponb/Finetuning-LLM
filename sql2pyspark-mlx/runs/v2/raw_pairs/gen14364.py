from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") == "US") & ((F.col("device") == "mobile") | (F.to_date("started_at") >= "2024-07-01")))
    .select(
        "id",
        "customer_id",
        "device",
        F.trim("country").alias("clean_country"),
        F.dayofweek("started_at").alias("started_at_dayofweek"),
    )
)
