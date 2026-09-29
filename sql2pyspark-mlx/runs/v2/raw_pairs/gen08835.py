from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id").isNotNull() & (F.col("amount") > 810)) | (F.col("order_id") == 31))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
)
