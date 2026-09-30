from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("product_name").like("%Bot%"))

products_stats = (
    valid_products
    .groupBy("brand")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.countDistinct("price").alias("unique_suppliers"),
    )
)

result = (
    products_stats
    .select("brand", "stddev_cost", "unique_suppliers")
    .orderBy("brand")
)
