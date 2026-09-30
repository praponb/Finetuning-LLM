from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("channel") != "store").alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.opened_date"))
    .orderBy("city", "id")
    .limit(3)
)
