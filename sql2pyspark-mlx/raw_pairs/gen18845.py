from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("customer_id").isin([5, 17]))
    .withColumn("channel_status", F.concat("channel", F.lit("/"), "status"))
    .withColumn("status_group", F.when(F.col("status").isin(["COMPLETED", "RETURNED"]), "Priority").otherwise("Other"))
)

result = (
    cleaned
    .groupBy("customer_id", "status")
    .pivot("channel", ["mobile", "web"])
    .agg(F.max("amount"))
)
