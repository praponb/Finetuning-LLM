from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").isNotNull() & F.col("amount").between(830, 970))
    .groupBy("method")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") >= 40)
)
