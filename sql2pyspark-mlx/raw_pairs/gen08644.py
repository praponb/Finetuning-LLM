from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 5, 6]))
    .groupBy("product_id")
    .agg(
        F.min("units").alias("min_units"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.stddev_units") > 23)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.cost"),
        F.col("t.min_units"),
        F.col("t.stddev_units"),
    )
    .orderBy("id", "stddev_units")
)
