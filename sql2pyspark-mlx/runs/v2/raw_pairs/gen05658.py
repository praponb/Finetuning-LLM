from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
    .orderBy(F.col("avg_stock_qty").desc(), "category")
    .limit(1)
)
