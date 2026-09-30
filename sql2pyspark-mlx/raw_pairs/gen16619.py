from pyspark.sql import functions as F

filtered_products = df_products.filter(F.col("price") > 710)

agg = (
    filtered_products
    .groupBy("brand")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)

result = agg
