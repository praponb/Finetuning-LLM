from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") <= 410) & (F.col("order_date") <= "2025-01-01"))
    .select(
        "id",
        "shipping_fee",
        (F.col("shipping_fee") / F.nullif("discount", F.lit(0))).alias("shipping_fee_per_discount"),
    )
)
