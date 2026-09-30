from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(200, 480))
    .groupBy("brand")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
)
