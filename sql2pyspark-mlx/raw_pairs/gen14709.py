from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("product_name").like("%Maker%"))
    .withColumn("cost_minus_stock_qty", (F.col("cost") - F.col("stock_qty")))
)

products_stats = (
    recent_products
    .groupBy("category")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
    )
)

result = (
    products_stats
    .filter(F.col("acme_count") < 4)
    .select("category", "category_values", "acme_count")
    .orderBy(F.col("category_values").desc())
)
