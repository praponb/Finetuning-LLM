from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.supplier_id").isin([6, 26]))
    .groupBy(F.col("s.country"), F.col("s.city"))
    .agg(F.sort_array(F.collect_set(F.col("p.brand"))).alias("brand_list"))
)
