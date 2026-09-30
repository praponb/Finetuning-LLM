from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["SG", "JP"]))
    .select(
        "id",
        "device",
        "country",
        "customer_id",
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
    )
)
