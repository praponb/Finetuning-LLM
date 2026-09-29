from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["store", "partner", "web"])
    .agg(F.max("amount"))
)
