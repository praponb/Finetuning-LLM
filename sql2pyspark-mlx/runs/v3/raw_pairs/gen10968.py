from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1300)
    .select(
        "discount",
        "shipping_fee",
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
    )
    .orderBy(F.col("amount_with_shipping").desc(), "discount")
)
