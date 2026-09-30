from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "supplier_id")
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
    .orderBy(F.col("brand_values").desc(), F.col("avg_stock_qty").desc())
)
