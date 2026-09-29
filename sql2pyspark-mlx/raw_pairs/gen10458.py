from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .pivot("status", ["RETURNED", "PENDING", "CANCELLED"])
    .agg(F.avg("discount"))
)
