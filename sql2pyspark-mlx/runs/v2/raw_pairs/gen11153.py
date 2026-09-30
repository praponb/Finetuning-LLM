from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Des%"))
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)
