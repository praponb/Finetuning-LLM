from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([1, 10, 12]))
    .select(
        "discount",
        (F.col("shipping_fee") - F.col("amount")).alias("amount_with_shipping"),
    )
)
