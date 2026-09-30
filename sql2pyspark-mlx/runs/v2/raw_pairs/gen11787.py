from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.brand") == "Globex", 1).otherwise(0))) / F.count("*")), 1).alias("pct_globex"),
        F.sort_array(F.collect_set(F.col("p.brand"))).alias("brand_list"),
    )
)
