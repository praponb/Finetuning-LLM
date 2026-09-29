from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["store", "mobile", "partner", "web"])
    .agg(F.sum("amount"))
)
