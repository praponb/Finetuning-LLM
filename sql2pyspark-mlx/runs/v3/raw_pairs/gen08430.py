from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["partner", "web", "mobile", "store"])
    .agg(F.avg("amount"))
    .orderBy("status")
)
