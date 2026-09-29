from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") >= 0.15) & (F.col("status") == "RETURNED"))
    .select(
        "id",
        "channel",
        "discount",
        F.when(F.col("status").isin(["PENDING", "SHIPPED"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.when(F.col("discount") >= 0.3, "High").when(F.col("discount") >= 0.16, "Medium").otherwise("Low").alias("discount_level"),
    )
    .orderBy("channel")
)
