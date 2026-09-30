from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.store_id").isin([1, 3, 4, 6]))
    .groupBy(F.col("s.city"), F.col("s.country"))
    .agg(
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") > 5)
    .orderBy(F.col("row_count").desc(), "city", "country")
    .limit(5)
)
