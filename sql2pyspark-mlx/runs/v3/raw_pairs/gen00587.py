from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([11, 31, 38]))
    .groupBy("method")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") < 27)
    .orderBy(F.col("amount_range").desc(), "method")
)
