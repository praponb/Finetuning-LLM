from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("order_date") <= "2025-05-05").alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.region"), F.col("s.city"), F.col("s.opened_date"))
    .orderBy(F.col("city").desc(), "region", "id")
    .limit(10)
)
