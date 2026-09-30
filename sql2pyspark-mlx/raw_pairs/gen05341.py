from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.size(F.col("p.tags")) >= 2)
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.count(F.when(F.col("p.brand") == "Stark", 1)).alias("stark_count"),
        F.sort_array(F.collect_set(F.col("p.brand"))).alias("brand_list"),
    )
    .orderBy("id", "city")
    .limit(3)
)
