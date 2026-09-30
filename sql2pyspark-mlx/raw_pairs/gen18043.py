from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("channel") != "store"))
    .withColumn("discount_per_amount", F.round((F.col("discount") / F.col("amount")), 2))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    valid
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.city").alias("city"), F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.discount_per_amount")).alias("max_discount_per_amount"),
    )
    .orderBy("max_discount_per_amount")
)
