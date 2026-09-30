from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .groupBy("category", F.month("launch_date").alias("launch_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .orderBy(F.col("launch_month").desc(), "category")
)
