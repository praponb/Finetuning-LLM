from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("channel") == "partner", F.col("shipping_fee")).otherwise(0)).alias("partner_shipping_fee"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 3)
    .drop("row_count")
)
