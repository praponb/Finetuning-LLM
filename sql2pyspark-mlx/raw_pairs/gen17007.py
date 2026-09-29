from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["web", "mobile", "partner", "store"])
    .agg(F.count("amount"))
)
