from pyspark.sql import functions as F

recent_products = df_products.filter((F.size("tags") > 2) & (F.col("launch_date") < "2024-05-01"))

products_stats = (
    recent_products
    .groupBy("brand")
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)

result = products_stats.orderBy(F.col("brand").desc())
