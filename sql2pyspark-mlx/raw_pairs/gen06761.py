from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)
