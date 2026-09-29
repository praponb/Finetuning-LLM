from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("paid_at_day", F.col("paid_at").cast("date"))
    .withColumn("is_success", F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
)

result = (
    base
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("method").isNotNull())
        .select("status")
    )
)
