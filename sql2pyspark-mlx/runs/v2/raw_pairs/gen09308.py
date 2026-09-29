from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .filter(F.col("o.store_id").isNotNull())
    .join(df_stores.filter(F.col("opened_date") <= "2024-02-24").alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .select(F.col("o.id"), F.col("o.customer_id"))
)
