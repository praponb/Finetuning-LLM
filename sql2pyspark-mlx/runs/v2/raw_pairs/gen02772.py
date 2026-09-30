from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.min("launch_date").alias("first_launch_date"),
    )
    .orderBy("category_values", F.col("brand").desc(), "launch_quarter")
    .limit(1)
)
