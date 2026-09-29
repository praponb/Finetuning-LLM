from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-05-03", "2025-09-28") | (F.col("discount") > 0.33))
    .select(
        "id",
        "shipping_fee",
        F.when(F.col("channel").isin(["web", "mobile"]), "Tier 1").otherwise("Other").alias("channel_group"),
        (F.col("discount") - F.col("shipping_fee")).alias("discount_minus_shipping_fee"),
    )
)
