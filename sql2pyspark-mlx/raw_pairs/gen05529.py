from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Safari")
    .select(
        "id",
        "device",
        "customer_id",
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
        F.to_date("started_at").alias("started_at_date"),
    )
    .orderBy("device")
)
