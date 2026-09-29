from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("shipping_fee") < 23) & (F.col("amount") >= 1160))
    .select(
        "discount",
        "amount",
        F.when(F.col("channel").isin(["store", "partner"]), "Priority").otherwise("Other").alias("channel_group"),
    )
)
