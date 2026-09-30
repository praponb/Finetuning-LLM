from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "PENDING", "RETURNED"]) | F.col("amount").isNull())
    .groupBy("status", F.year("order_date").alias("order_year"))
    .agg(
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"),
        F.max("shipping_fee").alias("max_shipping_fee"),
    )
    .filter(F.col("max_shipping_fee") < 23)
    .orderBy(F.col("order_year").desc())
)
