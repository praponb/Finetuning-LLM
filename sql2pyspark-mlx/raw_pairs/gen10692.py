from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([14, 18, 24, 29]) & F.col("status").isin(["CANCELLED", "RETURNED", "PENDING"]))
    .orderBy("amount")
)
