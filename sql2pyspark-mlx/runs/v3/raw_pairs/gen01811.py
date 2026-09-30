from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.max("cost").alias("max_cost"),
    )
)
