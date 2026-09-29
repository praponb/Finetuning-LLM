from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.quantity")).alias("total_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("category").desc_nulls_last())
)
