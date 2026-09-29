from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.lower("store_name").like("%ld %")).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("i.last_restock").between("2024-11-01", "2025-05-01"))
    .select(F.col("i.id"), F.col("i.quantity"))
    .orderBy(F.col("id").desc(), "quantity")
)
