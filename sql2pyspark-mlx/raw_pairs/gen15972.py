from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["brand", "category"], ["brand"]],
        F.col("p.brand").alias("brand"),
        F.col("p.category").alias("category"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("brand").asc_nulls_last())
)
