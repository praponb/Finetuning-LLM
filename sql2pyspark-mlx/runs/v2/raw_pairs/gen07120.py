from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.min_cost") >= 660)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        F.col("t.min_cost"),
        F.col("t.brand_values"),
    )
    .orderBy(F.col("id").desc())
)
