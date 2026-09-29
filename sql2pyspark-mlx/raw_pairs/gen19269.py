from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand").alias("brand"), F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.unit_price")).alias("max_unit_price"),
        F.max(F.col("s.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("category").asc_nulls_last())
)
