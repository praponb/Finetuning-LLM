from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.brand"),
        F.add_months(F.col("i.last_restock"), 3).alias("last_restock_plus_months"),
    )
    .orderBy("product_id")
)
