from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"), "brand")
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .filter(F.col("avg_stock_qty") < 31)
    .orderBy(F.col("avg_stock_qty").desc())
)
