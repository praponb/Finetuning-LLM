from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.count("stock_qty").alias("stock_qty_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("stock_qty_count") >= 3)
    .orderBy(F.col("stock_qty_count").desc())
)
