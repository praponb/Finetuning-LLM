from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") < "2025-07-31")
    .groupBy("status")
    .pivot("channel", ["web", "partner", "mobile"])
    .agg(F.count("shipping_fee"))
    .orderBy("status")
)
