from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2025-01-01")
    .select(
        "id",
        "started_at",
        F.date_trunc("MONTH", "started_at").alias("started_at_period"),
    )
)
