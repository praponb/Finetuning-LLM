from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.26)
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.count("*").alias("order_count"),
        F.countDistinct("shipping_fee").alias("unique_customers"),
    )
    .filter(F.col("unique_customers") < 5)
    .drop("unique_customers")
    .orderBy("order_count")
)
