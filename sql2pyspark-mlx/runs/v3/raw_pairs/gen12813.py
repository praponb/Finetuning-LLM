from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
    .orderBy(F.col("stddev_amount").desc())
)
