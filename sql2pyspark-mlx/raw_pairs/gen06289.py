from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 16)
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.min("discount").alias("min_discount"),
        F.max("discount").alias("max_discount"),
    )
    .filter(F.col("min_discount") < 0.36)
    .orderBy(F.col("order_year").desc())
    .limit(1)
)
