from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc_nulls_last())
)
