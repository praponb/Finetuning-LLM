from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.city"), F.col("t.brand_list"))
    .orderBy("city")
)
