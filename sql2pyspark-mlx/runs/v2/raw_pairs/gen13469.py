from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "country",
        "customer_id",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        F.when(F.col("device").isin(["tablet", "desktop"]), "Tier 1").otherwise("Other").alias("device_group"),
    )
    .orderBy("started_at_label", "id")
    .limit(20)
)
