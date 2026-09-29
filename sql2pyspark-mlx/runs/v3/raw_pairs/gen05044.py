from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.countDistinct(F.col("o.discount")).alias("unique_statuss"),
    )
    .filter(F.col("order_count") >= 1)
    .orderBy(F.col("id").desc())
    .limit(20)
)
