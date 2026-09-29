from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.category").alias("category"), F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
