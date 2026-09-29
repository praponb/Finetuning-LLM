from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("products.brand").isin(["Initech", "Stark"]))
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.2, "high").when(F.col("suppliers.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min(F.col("products.launch_date")).alias("min_launch_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("products.category")))).alias("category_values"),
    )
    .orderBy(F.col("rating_bucket").desc())
)
