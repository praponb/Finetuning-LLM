from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["category", "brand"], ["category"], []],
        F.col("d.category").alias("category"),
        F.col("d.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("category").asc_nulls_last())
)
