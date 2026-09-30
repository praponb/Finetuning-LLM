from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5, 6]))
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("pt.unique_products"),
        F.col("pt.stddev_units"),
    )
    .orderBy(F.col("unique_products").desc(), "id")
)
