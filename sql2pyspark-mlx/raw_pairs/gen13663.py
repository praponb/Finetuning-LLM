from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") > 25) & ((F.year("order_date") == 2025) | F.col("discount").between(0.24, 0.25)))
    .select(
        "shipping_fee",
        "status",
        "discount",
        F.col("amount").cast("int").alias("amount_int"),
    )
    .orderBy("shipping_fee")
)
