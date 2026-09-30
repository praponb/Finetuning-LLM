from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .groupBy(
        F.col("suppliers.city"),
        F.when(F.col("suppliers.rating") >= 3.8, "high").when(F.col("suppliers.rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("products.brand"))).alias("brand_list"),
        F.sum(F.when(F.col("products.brand") == "Stark", F.col("products.price")).otherwise(0)).alias("stark_price"),
    )
    .filter(F.col("stark_price") >= 62)
    .orderBy("city", "rating_bucket")
    .limit(50)
)
