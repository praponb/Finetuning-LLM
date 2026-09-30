from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("cost").between(440, 610)).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .filter(F.month(F.col("i.last_restock")).isin([4, 7, 12]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("i.product_id"),
    )
    .orderBy(F.col("store_id").desc(), "id")
    .limit(3)
)
