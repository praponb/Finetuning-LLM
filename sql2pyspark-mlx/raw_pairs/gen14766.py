from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.order_date").between("2023-11-01", "2024-01-01"))
    .groupBy(F.col("s.id"), F.col("s.region"))
    .agg(F.count(F.col("o.id")).alias("order_count"))
    .orderBy(F.col("order_count").desc(), "id")
)
