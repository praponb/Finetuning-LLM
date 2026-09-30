from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([4, 13, 21, 24]))
    .select("id", "method", "order_id", "amount")
    .orderBy("order_id")
)
