from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_store_sales"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("pt.num_store_sales"),
        F.col("pt.stddev_units"),
    )
)
