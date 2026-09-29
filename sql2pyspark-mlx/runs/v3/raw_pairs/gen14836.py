from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter(F.col("category").isin(["Electronics", "Home"]) & F.col("product_name").isNotNull() & ~F.col("brand").isin(["Umbrella", "Initech", "Globex"]))
)

products_stats = (
    valid_products
    .groupBy("brand", F.month("launch_date").alias("launch_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)

result = (
    products_stats
    .select("brand", "launch_month", "category_values", "latest_launch_date")
    .orderBy(F.col("category_values").desc())
)
