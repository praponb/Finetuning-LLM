from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .withColumn("amount_minus_discount", (F.col("amount") - F.col("discount")))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    filtered
    .groupBy("customer_id")
    .pivot("channel", ["mobile", "store", "web"])
    .agg(F.avg("amount").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
