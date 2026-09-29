from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(~F.col("o.channel").isin(["partner", "mobile", "store"]))
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(F.count(F.col("o.id")).alias("order_count"))
    .filter(F.col("order_count") > 5)
)
