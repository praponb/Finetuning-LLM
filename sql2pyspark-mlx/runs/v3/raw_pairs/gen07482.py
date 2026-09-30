from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter((F.col("p.stock_qty") < 247) & F.col("p.price").between(570, 670))
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
    )
    .orderBy("category")
)
