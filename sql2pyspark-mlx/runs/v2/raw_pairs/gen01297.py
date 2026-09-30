from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 1240)
    .groupBy("channel")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.max("amount").alias("max_amount"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
    .orderBy("channel")
)
