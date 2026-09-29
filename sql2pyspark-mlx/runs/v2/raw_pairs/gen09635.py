from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.year("order_date") == 2023).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .filter(F.col("s.store_name").like("Ai%"))
    .select(F.col("s.id"), F.col("s.city"), F.col("s.region"), F.col("s.opened_date"))
)
