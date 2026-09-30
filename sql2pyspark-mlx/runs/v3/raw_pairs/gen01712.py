from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id").isNotNull() & (F.col("discount") >= 0.27)) | (F.col("status") == "CANCELLED"))
    .select("id", "channel", "customer_id")
    .orderBy("id")
    .limit(5)
)
