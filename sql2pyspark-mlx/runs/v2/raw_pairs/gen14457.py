from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.stddev_price") >= 58)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("t.category_list"),
        F.col("t.stddev_price"),
    )
    .orderBy("id")
)
