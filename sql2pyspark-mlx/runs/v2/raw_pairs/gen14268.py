from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel").isin(["partner", "mobile", "web"]) & (F.col("amount") > 1100)) | F.col("shipping_fee").isNotNull())
    .groupBy("status", F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
    .orderBy("status")
)
