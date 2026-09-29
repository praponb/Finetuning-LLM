from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.19, 0.32))
    .groupBy(
        F.quarter("order_date").alias("order_quarter"),
        F.year("order_date").alias("order_year"),
    )
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.countDistinct("discount").alias("unique_customers"),
        F.avg("discount").alias("avg_discount"),
    )
    .orderBy(F.col("order_quarter").desc())
)
