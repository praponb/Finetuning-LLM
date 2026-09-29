from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.year("launch_date").alias("launch_year"), "brand")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)
