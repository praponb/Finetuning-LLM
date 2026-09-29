from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
)
