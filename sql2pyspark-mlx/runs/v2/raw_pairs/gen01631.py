from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") < 0.12) & F.col("status").isin(["RETURNED", "PENDING", "COMPLETED"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "shipping_fee",
        F.initcap(F.trim("status")).alias("status_title"),
        F.when(F.col("status").isin(["COMPLETED", "SHIPPED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)
