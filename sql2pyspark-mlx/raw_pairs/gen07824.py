from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Globex"]))
    .groupBy("supplier_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.country"), F.col("s.city"), F.col("t.brand_values"))
)
