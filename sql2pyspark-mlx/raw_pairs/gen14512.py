from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("cost") > 310)
    .groupBy("supplier_id")
    .agg(
        F.min("price").alias("min_price"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.min_price") > 870)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("t.min_price"),
        F.col("t.category_list"),
    )
    .orderBy("id")
    .limit(1)
)
