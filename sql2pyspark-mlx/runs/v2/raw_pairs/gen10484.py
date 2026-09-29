from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Firefox") & F.col("customer_id").isNotNull())
    .select(
        "id",
        "started_at",
        "customer_id",
        "country",
        F.hour("started_at").alias("started_at_hour"),
    )
)
