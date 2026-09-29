from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "amount",
        F.when(F.col("status").isin(["PENDING", "SHIPPED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
    .orderBy("amount", "status_group")
)
