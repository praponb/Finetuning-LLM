from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .withColumn("method_title", F.initcap(F.trim("method")))
    .withColumn("method_filled", F.coalesce("method", F.lit("OTHER")))
)

result = (
    valid
    .groupBy("status", "order_id")
    .pivot("method", ["cash", "paypal", "card"])
    .agg(F.max("amount").alias("amount"))
)
