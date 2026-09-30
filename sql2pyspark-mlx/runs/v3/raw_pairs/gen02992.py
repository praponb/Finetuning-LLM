from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.filter(F.col("brand").isin(["Initech", "Globex"])).alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left_anti")
    .select(F.col("inventory.id"), F.col("inventory.store_id"))
)
