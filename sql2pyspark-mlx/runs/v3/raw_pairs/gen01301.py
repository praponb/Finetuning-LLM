from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(560, 1360) & (F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]) | (F.col("method") != "cash")))
    .select("id", "amount", "order_id", F.round("amount", 2).alias("amount_rounded"))
)
