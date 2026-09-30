from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["mobile", "partner", "web"])
    .agg(F.avg("discount"))
)
