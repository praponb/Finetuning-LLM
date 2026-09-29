from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("s.sqft") >= 12732)
    .groupBy(F.col("o.store_id"))
    .agg(F.countDistinct(F.col("p.amount")).alias("unique_methods"))
    .orderBy("store_id")
)
