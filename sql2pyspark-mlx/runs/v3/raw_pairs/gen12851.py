from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("stock_qty").isNotNull())
    .groupBy("supplier_id")
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.city"), F.col("t.brand_list"))
)
