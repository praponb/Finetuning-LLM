from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.filter(F.col("opened_date").between("2024-03-01", "2025-09-02")).alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left_semi")
    .filter(F.col("orders.discount") <= 0.39)
    .select(F.col("orders.id"), F.col("orders.channel"), F.col("orders.status"))
)
