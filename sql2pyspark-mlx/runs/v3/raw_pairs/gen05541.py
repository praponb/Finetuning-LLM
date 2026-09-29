from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") < "2025-08-01") & F.col("customer_id").isin([9, 12, 31]) & F.col("browser").isin(["Firefox", "Chrome", "Edge"]))
    .select(
        "customer_id",
        "browser",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
    )
    .orderBy("browser")
)
