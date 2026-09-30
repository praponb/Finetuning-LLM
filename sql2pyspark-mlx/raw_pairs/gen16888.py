from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") > "2023-07-29") & (F.col("shipping_fee") > 29))
    .groupBy("status")
    .agg(
        F.countDistinct("amount").alias("unique_stores"),
        F.count("discount").alias("discount_count"),
    )
)
