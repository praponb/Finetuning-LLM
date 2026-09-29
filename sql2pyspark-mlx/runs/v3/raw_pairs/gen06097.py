from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1130) & (F.col("order_id") == 40) & F.col("order_id").isin([24, 31, 35, 37]))
    .withColumn("paid_at_day", F.col("paid_at").cast("date"))
    .withColumn("paid_at_period", F.date_trunc("year", "paid_at"))
    .orderBy("paid_at_day")
)
