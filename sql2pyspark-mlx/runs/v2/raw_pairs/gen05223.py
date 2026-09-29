from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("S.min_launch_date"),
        F.col("S.brand_values"),
    )
    .orderBy("brand_values", F.col("id").desc())
)
