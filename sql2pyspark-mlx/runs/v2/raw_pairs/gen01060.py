from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "sale") & F.col("i.quantity").between(114, 202))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
)
