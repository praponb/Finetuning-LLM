from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
    .filter(F.col("stddev_stock_qty") < 29)
    .select("brand", "category_values", "stddev_stock_qty")
    .orderBy(F.col("stddev_stock_qty").desc())
)
