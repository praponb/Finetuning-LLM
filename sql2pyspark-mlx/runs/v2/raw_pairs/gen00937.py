from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 1380) & (F.col("order_id").isin([7, 37]) | F.col("status").isin(["SUCCESS", "FAILED"])))
    .select("method", "status", "amount")
)
