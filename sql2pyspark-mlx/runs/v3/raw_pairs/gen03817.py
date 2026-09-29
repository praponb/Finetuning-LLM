from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("discount")).otherwise(0)).alias("partner_discount"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("web_count") > 3)
)
