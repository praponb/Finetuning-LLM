from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.brand").alias("brand"), F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
