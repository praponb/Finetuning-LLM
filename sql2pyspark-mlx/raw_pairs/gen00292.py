from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Sports")
    .groupBy("brand", "category")
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)
