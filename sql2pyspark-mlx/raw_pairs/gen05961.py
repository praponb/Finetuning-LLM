from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Firefox")
    .select(
        "id",
        "device",
        "customer_id",
        "browser",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
    )
)
