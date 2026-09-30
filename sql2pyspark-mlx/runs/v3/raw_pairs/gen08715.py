from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(~F.col("product_name").like("%Running%"))
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("product_count"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("t.product_count"),
        F.col("t.category_list"),
    )
)
