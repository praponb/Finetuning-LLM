from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"), "brand")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
    .orderBy("category_values", "launch_year", "brand")
    .limit(3)
)
