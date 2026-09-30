from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(
        F.count("*").alias("num_orders"),
        F.sum(F.when(F.col("o.channel") == "mobile", F.col("o.amount")).otherwise(0)).alias("mobile_amount"),
    )
    .orderBy(F.col("mobile_amount").desc(), "city")
)
