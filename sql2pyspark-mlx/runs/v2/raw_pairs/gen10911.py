from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 17)
    .groupBy(F.quarter("launch_date").alias("launch_quarter"), "brand")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.max("cost").alias("max_cost"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)
