from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("order_id").isin([20, 28, 38]) & (F.col("amount") >= 940)))
    .select("amount", "status")
    .orderBy(F.col("status").desc(), F.col("amount").desc())
)
