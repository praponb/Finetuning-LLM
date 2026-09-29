from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy(F.col("amount_range").desc())
)
