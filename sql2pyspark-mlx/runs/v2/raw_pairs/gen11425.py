from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "DELIVERED"]) & F.col("order_id").isNull())
    .select("order_id", "status")
)
