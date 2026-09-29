from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([5, 6, 12, 37]) & (F.col("amount").isNull() | (F.col("method") == "promptpay")))
    .select("id", "order_id", F.initcap(F.trim("status")).alias("status_title"))
    .orderBy(F.col("id").desc(), F.col("order_id").desc())
)
