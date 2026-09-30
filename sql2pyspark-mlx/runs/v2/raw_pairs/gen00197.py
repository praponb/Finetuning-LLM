from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") < 30) & F.col("discount").between(0.11, 0.18))
    .select(
        "id",
        "customer_id",
        (F.col("amount") * F.col("shipping_fee")).alias("amount_with_shipping"),
    )
    .orderBy(F.col("amount_with_shipping").desc())
)
