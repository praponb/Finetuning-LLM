from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "RETURNED")
    .groupBy("status")
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.sum(F.when(F.col("channel") == "web", F.col("amount")).otherwise(0)).alias("web_amount"),
    )
    .filter(F.col("web_amount") >= 78)
    .orderBy("web_amount", F.col("shipping_fee_count").desc())
)
