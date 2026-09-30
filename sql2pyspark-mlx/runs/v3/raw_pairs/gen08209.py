from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile", "partner"]) & (F.col("customer_id").isin([13, 22, 31, 39]) | (F.col("customer_id") == 13)))
    .select("id", "status")
)
