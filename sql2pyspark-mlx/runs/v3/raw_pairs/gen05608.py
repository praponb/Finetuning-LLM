from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("price") > 770)
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(
        F.col("s.city"),
        F.col("s.rating"),
        F.col("pbs.category_values"),
        F.col("pbs.brand_list"),
    )
    .orderBy(F.col("rating").desc())
)
