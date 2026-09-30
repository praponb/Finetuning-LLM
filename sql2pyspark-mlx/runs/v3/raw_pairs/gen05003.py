from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .pivot("status", ["IN_TRANSIT", "RETURNED", "DELIVERED", "LOST"])
    .agg(F.sum("cost"))
    .orderBy("order_id")
)
