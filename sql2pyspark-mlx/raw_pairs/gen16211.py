from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNull() & F.col("store_id").isNotNull())
    .groupBy("status", F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("channel") == "store", F.col("amount")).otherwise(0)).alias("store_amount"),
        F.max("discount").alias("max_discount"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
    )
    .orderBy(F.col("pct_mobile").desc())
)
