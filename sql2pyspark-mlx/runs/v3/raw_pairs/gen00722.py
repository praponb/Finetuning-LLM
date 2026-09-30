from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 630)
    .groupBy("customer_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") > 28)
    .select("customer_id", "amount_range")
    .orderBy(F.col("customer_id").desc())
)
