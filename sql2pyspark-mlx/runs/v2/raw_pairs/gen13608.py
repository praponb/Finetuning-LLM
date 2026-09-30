from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "sale") | (F.col("brand") != "Stark"))
    .groupBy("brand")
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)
