from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(F.col("customer_id").isin([14, 15, 26, 33]) | ~F.col("device").isin(["tablet", "desktop"])))
    .select(
        "customer_id",
        "started_at",
        F.when(F.col("device").isin(["tablet", "mobile"]), "Priority").otherwise("Other").alias("device_group"),
    )
    .orderBy("device_group")
)
