from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 39)
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.max("discount").alias("max_discount"),
        F.countDistinct("discount").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") > 3)
    .drop("unique_stores")
    .orderBy(F.col("max_discount").desc(), "order_month")
)
