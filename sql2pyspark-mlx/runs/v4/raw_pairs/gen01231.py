from pyspark.sql import functions as F

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-04-09").cast("date")) & F.col("method").isNotNull())
    .withColumn("method_title", F.initcap(F.trim("method")))
)

result = (
    valid
    .groupBy(
        F.when(F.col("amount") >= 1560, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
    )
)
