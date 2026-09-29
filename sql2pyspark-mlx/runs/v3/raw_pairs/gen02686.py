from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~(((F.col("status") == "PENDING") & (F.col("shipping_fee") <= 25)) | (F.col("amount") > 1360)))
    .withColumn("days_since_order_date", F.datediff(F.current_date(), "order_date"))
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.44, "High").when(F.col("discount") >= 0.05, "Medium").otherwise("Low"))
    .orderBy(F.col("discount_tier").desc(), "id")
    .limit(10)
)
