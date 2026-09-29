from pyspark.sql import functions as F

recent_payments = df_payments.filter(F.col("order_id").isNotNull())

agg = (
    recent_payments
    .groupBy("method", "status")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
)

result = agg.filter(F.col("success_count") < 67)
