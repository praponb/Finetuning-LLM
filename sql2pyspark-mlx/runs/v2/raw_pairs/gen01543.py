from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(560, 780))
    .groupBy("status")
    .pivot("channel", ["web", "partner", "store", "mobile"])
    .agg(F.sum("shipping_fee"))
)
