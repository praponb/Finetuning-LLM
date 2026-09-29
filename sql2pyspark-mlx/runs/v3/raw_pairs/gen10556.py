from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(
        F.when(F.col("o.channel").isin(["mobile", "partner"]), "tier_1").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.sum(F.when(F.col("p.status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("success_count").desc())
)
