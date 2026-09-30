from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "paypal", "cash"]) & F.col("amount").between(1430, 1480))
    .withColumn("method_status", F.concat("method", F.lit(" - "), "status"))
)

agg = (
    valid_payments
    .groupBy("order_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    agg
    .select("order_id", "amount_range")
    .orderBy(F.col("amount_range").desc())
)
