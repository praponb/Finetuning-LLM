from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "paypal"]))
    .groupBy("method")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") >= 47)
    .select("method", "amount_range")
    .orderBy("amount_range")
)
