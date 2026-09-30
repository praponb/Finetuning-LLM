from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id").isin([5, 7]) & (F.col("status") == "SUCCESS")) | F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
    .select("id", "order_id")
    .orderBy(F.col("id").desc())
    .limit(10)
)
