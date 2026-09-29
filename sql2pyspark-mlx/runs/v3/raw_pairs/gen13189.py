from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~(F.col("status").isin(["CANCELLED", "PENDING", "RETURNED"]) | (F.col("customer_id") == 36)))
    .select("discount", F.lower("status").alias("status_lower"))
)
