from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .groupBy(F.year(F.col("stores.opened_date")).alias("opened_year"))
    .agg(
        F.min(F.col("inventory.quantity")).alias("min_quantity"),
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
    )
    .orderBy("total_quantity", "opened_year")
    .limit(50)
)
