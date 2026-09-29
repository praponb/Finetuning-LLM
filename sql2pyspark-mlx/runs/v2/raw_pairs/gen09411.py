from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") > 840)
    .select("channel", "max_amount")
    .orderBy(F.col("max_amount").desc(), "channel")
    .limit(1)
)
