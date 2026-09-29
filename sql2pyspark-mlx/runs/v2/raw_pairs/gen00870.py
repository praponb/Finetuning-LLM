from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter((F.col("p.cost") < 460) | F.col("i.product_id").isin([1, 5, 6, 7]))
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.category"),
        F.col("p.cost"),
        F.last_day(F.col("i.last_restock")).alias("last_restock_month_end"),
    )
    .orderBy("product_id", F.col("cost").desc())
)
