from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "SG")
    .groupBy(
        F.when(F.col("s.country").isin(["SG", "JP"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.brand")))).alias("brand_values"),
    )
    .orderBy("brand_values", F.col("country_group").desc())
)
