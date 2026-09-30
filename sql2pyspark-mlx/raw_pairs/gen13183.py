from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("a.units")), 2).alias("avg_units"),
    )
    .orderBy(F.col("brand").desc())
)
