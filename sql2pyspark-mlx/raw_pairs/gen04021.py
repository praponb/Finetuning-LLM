from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.stock_qty") == 132)
    .groupBy(
        F.when(F.col("p.category").isin(["Home", "Grocery"]), "core").otherwise("other").alias("category_group"),
    )
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
    .orderBy(F.col("unique_stores").desc())
)
