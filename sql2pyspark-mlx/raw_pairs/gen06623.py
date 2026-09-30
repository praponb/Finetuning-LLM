from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.size("tags") >= 1)
    .groupBy("supplier_id")
    .agg(F.sort_array(F.collect_set("category")).alias("category_list"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("t.category_list"))
    .orderBy(F.col("city").desc())
)
