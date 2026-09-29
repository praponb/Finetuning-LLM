from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]) & F.col("country").isin(["DE", "JP"]) & (F.col("browser") == "Safari"))
    .select(
        "started_at",
        "browser",
        "device",
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
    )
)
