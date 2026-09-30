from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.initech_count") > 5)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("ss.brand_values"),
        F.col("ss.initech_count"),
    )
)
