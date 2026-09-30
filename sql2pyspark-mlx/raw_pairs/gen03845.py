from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 238) & F.month("last_restock").isin([5, 6, 10]))
    .groupBy(
        F.month("last_restock").alias("last_restock_month"),
        "product_id",
        "store_id",
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("inventory_count"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .orderBy(F.col("product_id").desc(), "last_restock_month", "store_id")
    .limit(5)
)
