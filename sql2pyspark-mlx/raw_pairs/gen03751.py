from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.supplier_id") == 27)
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        F.col("d.product_name"),
        F.col("d.stock_qty"),
    )
)
