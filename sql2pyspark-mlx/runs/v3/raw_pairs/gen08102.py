from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") >= "2023-03-01") & (F.size("tags") >= 2) & (F.col("stock_qty") < 391))
    .groupBy("brand")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .orderBy("avg_cost", "brand")
    .limit(5)
)
