from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(F.sort_array(F.collect_set("category")).alias("category_list"))
    .orderBy(F.col("launch_year").desc())
)
