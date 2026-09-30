from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "outer")
    .filter(F.col("stores.opened_date").between("2024-01-22", "2024-12-01"))
    .select(F.col("orders.amount"), F.col("stores.city"))
)
