from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.max("units").alias("max_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("ps.stddev_units"),
        F.col("ps.max_units"),
    )
    .orderBy("stddev_units")
)
