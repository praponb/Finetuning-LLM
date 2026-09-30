from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.max("discount").alias("max_discount"),
    )
    .filter(F.col("stddev_amount") >= 51)
    .orderBy(F.col("max_discount").desc())
)
