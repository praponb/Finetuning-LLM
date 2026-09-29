from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("opened_date").between("2024-10-01", "2024-11-17")).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("i.product_id").isin([2, 10]))
    .select(F.col("i.id"), F.col("i.product_id"))
)
