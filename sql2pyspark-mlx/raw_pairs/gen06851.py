from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.quantity") >= 205)
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("product_name").desc(), "id")
    .limit(10)
)
