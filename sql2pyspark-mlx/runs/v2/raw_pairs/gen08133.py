from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.filter(F.col("quantity").between(85, 120)).alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left_semi")
    .filter(F.col("products.price") < 390)
    .select(
        F.col("products.id"),
        F.col("products.launch_date"),
        F.col("products.price"),
    )
)
