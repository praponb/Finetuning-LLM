from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Safari") & (F.to_date("started_at") >= "2025-05-01"))
    .select(
        "id",
        "customer_id",
        F.hour("started_at").alias("started_at_hour"),
        F.when(F.col("device").isin(["desktop", "tablet"]), "Group A").otherwise("Other").alias("device_group"),
    )
    .orderBy(F.col("customer_id").desc())
)
