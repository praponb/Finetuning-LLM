from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.avg("price").alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.avg_price") > 400)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("ss.brand_values"),
        F.col("ss.avg_price"),
    )
    .orderBy("brand_values")
)
