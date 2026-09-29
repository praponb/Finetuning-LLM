from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Maker%") | F.col("brand").isNotNull())
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
    .orderBy(F.col("brand_values").desc())
)
