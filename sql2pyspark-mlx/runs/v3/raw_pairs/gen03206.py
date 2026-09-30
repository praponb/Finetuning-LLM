from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([16, 17]))
    .select("id", "order_id", F.floor("amount").alias("amount_whole"))
    .orderBy(F.col("order_id").desc(), F.col("amount_whole").desc())
)
