from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.city").isin(["London", "Singapore"]), "priority").otherwise("other").alias("city_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.brand")))).alias("brand_values"),
    )
    .orderBy("brand_values", "city_group")
)
