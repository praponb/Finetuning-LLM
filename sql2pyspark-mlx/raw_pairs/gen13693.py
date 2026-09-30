from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("amount") < 450) & (F.col("method") != "paypal")) | (F.col("amount") >= 1370))
    .select("id", "order_id", "amount")
)
