from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "amount",
        "shipping_fee",
        "channel",
        F.when(F.col("status").isin(["PENDING", "RETURNED"]), "Group A").otherwise("Other").alias("status_group"),
    )
)
