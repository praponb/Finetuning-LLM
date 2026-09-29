from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([4, 25]) & F.col("order_id").isin([5, 19, 24, 32]))
    .select("id", "amount")
    .orderBy(F.col("id").desc())
)
