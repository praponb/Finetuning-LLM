from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("store_name").like("%Expr%")).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("i.quantity") >= 60)
    .select(F.col("i.id"), F.col("i.product_id"), F.col("i.last_restock"))
    .orderBy("last_restock", "id")
    .limit(10)
)
