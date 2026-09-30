from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg((F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"))
    .filter(F.col("shipping_fee_range") >= 21)
    .select("channel", "shipping_fee_range")
)
