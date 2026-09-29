from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(7, 10) & F.col("order_id").isin([4, 12, 13, 27]) & F.col("order_id").isNull())
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .orderBy("paid_at", "id")
    .limit(5)
)
