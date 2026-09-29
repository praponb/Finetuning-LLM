from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("category").asc_nulls_last())
)
