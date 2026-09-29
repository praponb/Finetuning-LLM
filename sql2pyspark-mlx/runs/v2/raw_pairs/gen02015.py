from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([14, 18, 22, 35]) & (F.to_date("paid_at") < "2023-05-01"))
    .groupBy("status")
    .agg(
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") > 1440)
    .drop("max_amount")
    .orderBy(F.col("min_amount").desc())
)
