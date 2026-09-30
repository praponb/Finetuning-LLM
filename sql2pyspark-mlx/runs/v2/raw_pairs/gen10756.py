from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5, 6]))
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(
        F.col("p.cost"),
        F.col("t.first_last_restock"),
        F.col("t.last_last_restock"),
    )
    .orderBy("first_last_restock", F.col("cost").desc())
)
