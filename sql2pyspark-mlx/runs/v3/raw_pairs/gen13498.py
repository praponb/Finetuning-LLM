from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("shipping_fee").alias("value"))
    .intersect(
        df_payments
        .filter(F.col("order_id").isin([2, 7]))
        .select(F.col("amount").alias("value"))
    )
)
