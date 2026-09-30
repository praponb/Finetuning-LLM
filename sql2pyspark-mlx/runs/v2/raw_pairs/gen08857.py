from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .pivot("channel", ["partner", "mobile"])
    .agg(F.count("shipping_fee"))
    .orderBy("order_quarter")
)
