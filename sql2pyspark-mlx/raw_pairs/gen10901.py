from pyspark.sql import functions as F

by_dimension = (
    df_order_items
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.category").alias("category"), F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.quantity")).alias("max_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    by_dimension
    .select("max_quantity")
    .orderBy(F.col("max_quantity").desc())
)
