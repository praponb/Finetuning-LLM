from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .select(
        "country",
        "customer_id",
        "started_at",
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
    )
)
