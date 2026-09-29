from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 5]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("min_quantity").desc_nulls_last())
)
