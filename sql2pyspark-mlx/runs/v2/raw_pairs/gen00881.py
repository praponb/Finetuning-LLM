from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("supplier_stats"), F.col("suppliers.id") == F.col("supplier_stats.supplier_id"), "inner")
    .select(
        F.col("suppliers.country"),
        F.col("suppliers.city"),
        F.col("supplier_stats.brand_values"),
    )
    .orderBy(F.col("brand_values").desc())
)
