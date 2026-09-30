from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") >= 27) & ((F.col("channel") == "mobile") | F.col("customer_id").isNull()))
    .groupBy("channel")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
    .filter(F.col("returned_count") >= 50)
)
