from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["web", "mobile"])
    .agg(F.max("discount"))
)
