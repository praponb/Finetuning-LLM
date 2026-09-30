from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Coffe%"))
    .groupBy("category")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") > 2700)
    .select("category", "category_values", "total_cost")
    .orderBy("total_cost", "category")
    .limit(3)
)
