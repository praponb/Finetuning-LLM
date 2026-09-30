from pyspark.sql import functions as F

filtered_products = df_products.filter(F.array_contains("tags", "eco"))

summary = (
    filtered_products
    .groupBy("category", "brand")
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)

result = summary.select("category", "brand", "brand_list")
