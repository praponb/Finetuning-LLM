from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([5, 13, 16, 32]) & ((F.col("customer_id") == 37) | (F.col("started_at") >= "2023-09-01")))
    .select(
        "id",
        "browser",
        "customer_id",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)
