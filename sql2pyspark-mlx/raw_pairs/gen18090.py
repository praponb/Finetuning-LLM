from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.array_contains("tags", "eco") & F.array_contains("tags", "sale"))
)

brand_summary = (
    recent_products
    .groupBy("brand")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)

result = (
    brand_summary
    .select("brand", "category_values", "total_stock_qty")
    .orderBy(F.col("total_stock_qty").desc(), F.col("brand").desc())
)
