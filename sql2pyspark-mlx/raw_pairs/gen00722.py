from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.unit_price")).alias("total_unit_price"),
    )
    .orderBy("category")
)
