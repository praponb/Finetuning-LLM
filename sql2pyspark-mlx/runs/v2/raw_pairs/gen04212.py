from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("product_name").like("R%"))

category_summary = (
    recent_products
    .groupBy("category", "brand")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    category_summary
    .filter(F.col("price_range") > 27)
    .orderBy(F.col("category").desc(), "brand")
    .limit(10)
)
