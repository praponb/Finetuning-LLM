from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Grocery")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
    .orderBy("brand_values")
)
