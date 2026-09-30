from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["SHIPPED", "COMPLETED"]) & (F.col("shipping_fee") < 15))
    .groupBy("status")
    .agg(F.count("discount").alias("discount_count"))
)
