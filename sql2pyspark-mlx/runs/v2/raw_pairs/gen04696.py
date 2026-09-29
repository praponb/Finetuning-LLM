from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.sort_array(F.collect_set(F.col("p.brand"))).alias("brand_list"),
    )
    .orderBy("city", "product_count")
)
