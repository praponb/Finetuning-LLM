from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .pivot("channel", ["web", "partner", "mobile"])
    .agg(F.max("amount"))
)
