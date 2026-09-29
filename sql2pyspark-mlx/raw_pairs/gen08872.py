from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand").alias("brand"), F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.unit_price")).alias("total_unit_price"),
        F.avg(F.col("e.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("total_unit_price").asc_nulls_last())
)
