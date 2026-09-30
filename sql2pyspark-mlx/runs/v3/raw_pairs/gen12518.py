from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2025-07-01")
    .select(
        "amount",
        F.when(F.col("channel").isin(["web", "mobile"]), "Core").otherwise("Other").alias("channel_group"),
    )
    .orderBy(F.col("channel_group").desc())
)
