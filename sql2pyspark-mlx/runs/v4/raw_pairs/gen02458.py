from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.col("opened_date").between("2024-03-10", "2024-11-27")).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("o.order_date") <= "2024-08-18")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.amount"), F.col("o.discount"))
)
