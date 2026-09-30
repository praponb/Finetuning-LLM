from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2024-10-01")
    .select(
        "customer_id",
        "country",
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
    )
)
