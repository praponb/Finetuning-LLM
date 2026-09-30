from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("product_id").isin([1, 5, 6, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

result = (
    base
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("s.quantity_rounded")).alias("avg_quantity_rounded"),
        F.sum(F.col("s.quantity")).alias("total_quantity"),
    )
)
