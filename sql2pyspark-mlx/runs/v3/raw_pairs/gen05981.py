from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("suppliers.country") == "SG")
    .groupBy(
        F.when(F.col("suppliers.city").isin(["Berlin", "Bangkok"]), "priority").otherwise("other").alias("city_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("products.category")))).alias("category_values"),
        F.sort_array(F.collect_set(F.col("products.brand"))).alias("brand_list"),
    )
)
