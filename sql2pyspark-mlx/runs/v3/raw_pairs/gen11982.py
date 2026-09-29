from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "COMPLETED"]) & ((F.col("status") != "COMPLETED") | F.col("customer_id").isin([6, 8, 13, 24])))
    .groupBy("channel")
    .agg((F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"))
    .filter(F.col("shipping_fee_range") < 63)
    .orderBy("channel")
)
