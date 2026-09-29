from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .filter(F.col("o.store_id").isNotNull())
    .join(df_stores.filter(F.col("opened_date").between("2023-07-12", "2024-09-13")).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.customer_id"))
)
