from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.product_name").like("%wir%"))
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.countDistinct(F.col("p.cost")).alias("unique_categorys"),
    )
    .orderBy(F.col("product_count").desc(), "country")
)
