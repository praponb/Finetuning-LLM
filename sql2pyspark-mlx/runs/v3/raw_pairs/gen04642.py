from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") <= 17) & F.col("channel").isin(["partner", "store", "web"]))
    .rollup(F.date_format("order_date", "yyyy-MM").alias("order_month"), "channel")
    .agg(F.min("discount").alias("min_discount"))
)
