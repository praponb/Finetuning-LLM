from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([11, 19, 27, 32]) & F.col("amount").between(720, 1200))
    .select("id", "method")
)
