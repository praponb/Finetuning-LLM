from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("products.brand"),
        F.col("products.launch_date"),
        F.col("suppliers.rating"),
        F.col("suppliers.city"),
        F.when(F.col("suppliers.city").isin(["Singapore", "Tokyo"]), "Group A").otherwise("Other").alias("city_group"),
    )
)
