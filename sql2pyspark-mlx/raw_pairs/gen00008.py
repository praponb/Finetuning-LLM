from pyspark.sql import functions as F

recent_payments = df_payments.filter(F.col("order_id") == 17)

agg = (
    recent_payments
    .groupBy("method")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    agg
    .filter(F.col("amount_range") > 35)
    .select("method", "amount_range")
    .orderBy(F.col("method").desc(), F.col("amount_range").desc())
)
