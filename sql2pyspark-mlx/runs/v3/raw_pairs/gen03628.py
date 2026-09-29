from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.min("cost").alias("min_cost"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
    .filter(F.col("min_cost") > 550)
    .orderBy(F.col("min_cost").desc())
)
