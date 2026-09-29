from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.id"), F.col("s.supplier_name"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.sort_array(F.collect_set(F.col("p.brand"))).alias("brand_list"),
        F.min(F.col("p.cost")).alias("min_cost"),
    )
    .orderBy("supplier_name")
)
