from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("cost") <= 390)
    .groupBy("supplier_id")
    .agg(F.sort_array(F.collect_set("category")).alias("category_list"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
        F.col("products_by_supplier.category_list"),
    )
)
