from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("status") == "REFUNDED")

agg = (
    filtered_payments
    .groupBy("order_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
)

result = (
    agg
    .filter(F.col("avg_amount") >= 1450)
    .orderBy(F.col("order_id").desc(), F.col("unique_statuss").desc())
)
