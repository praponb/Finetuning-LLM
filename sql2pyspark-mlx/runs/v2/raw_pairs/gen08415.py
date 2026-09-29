from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.stock_qty") != 369)
    .select(
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("p.stock_qty"),
        F.concat_ws("-", F.col("p.product_name"), F.col("p.category")).alias("product_name_category_key"),
    )
    .orderBy("product_name_category_key")
)
