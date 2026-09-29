from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.id"), F.col("s.supplier_name"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.countDistinct(F.col("p.stock_qty")).alias("unique_categorys"),
        F.max(F.col("p.cost")).alias("max_cost"),
    )
    .orderBy(F.col("id").desc())
)
