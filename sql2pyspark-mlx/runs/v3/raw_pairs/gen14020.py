from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter((F.col("discount") <= 0.35) | F.col("store_id").isin([24, 39]))
    .withColumn("status_group", F.when(F.col("status").isin(["COMPLETED", "PENDING"]), "Tier 1").otherwise("Other"))
)

summary = (
    filtered_orders
    .groupBy("channel")
    .agg(F.count("*").alias("total_count"))
)

result = (
    summary
    .filter(F.col("total_count") < 8)
    .select("channel", "total_count")
    .orderBy("total_count")
)
