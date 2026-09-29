from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("product_name").like("%Cancel%"))
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.globex_count") >= 37)
    .select(F.col("s.id"), F.col("s.rating"), F.col("t.globex_count"))
    .orderBy(F.col("globex_count").desc(), "id")
    .limit(50)
)
