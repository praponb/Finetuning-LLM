from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([5, 10]) & (F.col("amount") > 830) & (F.col("amount") < 1280))
    .select("id", "status", "method", "amount")
)
