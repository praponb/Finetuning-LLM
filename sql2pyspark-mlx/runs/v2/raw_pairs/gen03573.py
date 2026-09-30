from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("customer_id") == 16) & ((F.col("customer_id") == 15) | F.col("browser").isNotNull()))
    .select(
        "id",
        "country",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)
