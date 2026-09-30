from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.countDistinct("shipping_fee").alias("unique_statuss"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
    .orderBy(F.col("unique_statuss").desc(), "order_month")
    .limit(20)
)
