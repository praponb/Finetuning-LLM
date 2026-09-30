from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.max("discount").alias("max_discount"),
        F.countDistinct("shipping_fee").alias("unique_channels"),
    )
    .filter(F.col("max_discount") > 0.22)
)
