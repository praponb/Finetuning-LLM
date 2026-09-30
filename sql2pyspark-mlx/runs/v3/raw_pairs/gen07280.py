from pyspark.sql import functions as F

valid_products = df_products.filter(F.size("tags") >= 1)

summary = (
    valid_products
    .groupBy("brand", F.month("launch_date").alias("launch_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = (
    summary
    .orderBy("launch_month", "brand")
    .limit(1)
)
