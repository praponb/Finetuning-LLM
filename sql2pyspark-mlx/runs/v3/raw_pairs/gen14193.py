from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNull() & ((F.col("order_date") < "2023-06-01") | F.month("order_date").isin([3, 5, 6])))
    .select(
        "id",
        "customer_id",
        "channel",
        F.round((F.col("shipping_fee") * 1.07), 2).alias("shipping_fee_adjusted"),
    )
    .orderBy("id")
)
