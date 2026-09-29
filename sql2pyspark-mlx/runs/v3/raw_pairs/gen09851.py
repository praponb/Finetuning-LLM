from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("s.sqft") < 6575)
    .groupBy(F.col("s.region"))
    .agg(F.round(F.avg(F.col("o.amount")), 2).alias("avg_amount"))
    .orderBy("region")
)
