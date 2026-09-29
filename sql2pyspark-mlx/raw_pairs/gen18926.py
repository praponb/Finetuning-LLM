from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .select(
        F.col("p.launch_date"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
    )
    .orderBy(F.col("product_id").desc())
)
