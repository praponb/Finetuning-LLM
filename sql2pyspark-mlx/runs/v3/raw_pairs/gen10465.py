from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.supplier_name").like("%Pac%"))
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.avg(F.col("p.price")).alias("avg_price"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.category")))).alias("category_values"),
    )
    .filter(F.col("product_count") > 2)
)
