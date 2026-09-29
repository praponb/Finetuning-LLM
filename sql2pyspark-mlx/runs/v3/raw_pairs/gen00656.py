from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "outer")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .filter(F.col("products.launch_date") >= "2023-10-01")
    .select(
        F.col("inventory.quantity"),
        F.col("products.stock_qty"),
        F.col("products.launch_date"),
        F.col("stores.region"),
        F.col("stores.city"),
        F.sort_array(F.col("products.tags")).alias("sorted_tags"),
    )
    .orderBy(F.col("launch_date").desc())
)
