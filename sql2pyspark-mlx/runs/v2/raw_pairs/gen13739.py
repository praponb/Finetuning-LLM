from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("cost") <= 300)
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("pbs.category_values"),
    )
    .orderBy(F.col("rating").desc(), "id")
    .limit(10)
)
