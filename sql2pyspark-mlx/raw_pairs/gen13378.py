from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.when(F.col("shipping_fee") >= 33, "high").when(F.col("shipping_fee") >= 10, "mid").otherwise("low").alias("shipping_fee_bucket"))
    .pivot("channel", ["partner", "web"])
    .agg(F.sum("discount"))
)
