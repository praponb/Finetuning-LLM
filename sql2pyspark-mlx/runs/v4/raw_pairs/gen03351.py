from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("launch_date") <= "2023-08-07")
    .groupBy("supplier_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.rating"), F.col("ss.brand_values"))
)
