from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("method") == "paypal")
    .withColumn("paid_at_dayofweek", F.dayofweek("paid_at"))
)

agg = (
    filtered_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"), "order_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count("*").alias("row_count"),
    )
)

result = agg.orderBy(F.col("paid_hour").desc())
