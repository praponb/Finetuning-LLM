from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("suppliers.city"))
    .agg(
        F.count(F.col("products.id")).alias("product_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("products.brand")))).alias("brand_values"),
        F.avg(F.col("products.price")).alias("avg_price"),
    )
    .orderBy(F.col("product_count").desc(), "avg_price", "city")
    .limit(50)
)
