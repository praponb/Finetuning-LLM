from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull() & (F.col("discount") > 0.23))
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "SHIPPED"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
    )
    .orderBy("status_group")
)
