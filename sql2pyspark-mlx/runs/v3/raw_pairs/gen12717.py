from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("cost").between(330, 340))
    .groupBy("supplier_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("supplier_stats"), F.col("suppliers.id") == F.col("supplier_stats.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("supplier_stats.brand_values"),
    )
    .orderBy("brand_values", F.col("rating").desc())
)
