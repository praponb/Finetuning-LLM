from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("shipping_fee")).otherwise(0)).alias("mobile_shipping_fee"),
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
    .filter(F.col("pct_returned") >= 23)
    .select("status", "mobile_shipping_fee", "pct_returned")
)
