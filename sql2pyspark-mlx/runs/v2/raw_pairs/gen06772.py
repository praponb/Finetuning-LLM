from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "web", "store"]) & (F.col("shipping_fee") < 19))
    .groupBy("customer_id", "status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("amount").alias("unique_stores"),
        F.min("order_date").alias("first_order_date"),
    )
    .orderBy(F.col("customer_id").desc(), "status")
)
