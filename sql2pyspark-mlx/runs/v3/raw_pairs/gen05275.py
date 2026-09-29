from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") > "2024-12-28")
    .select(
        "shipping_fee",
        "amount",
        "discount",
        F.col("amount").cast("int").alias("amount_int"),
    )
)
