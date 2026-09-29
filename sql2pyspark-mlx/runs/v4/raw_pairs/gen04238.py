from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .filter(F.col("stores.sqft").between(11064, 13458))
    .select(
        F.col("inventory.store_id"),
        F.col("stores.city"),
        F.col("stores.opened_date"),
        F.col("products.cost"),
        F.col("products.launch_date"),
        F.datediff(F.current_date(), F.col("stores.opened_date")).alias("days_since_opened_date"),
    )
)
