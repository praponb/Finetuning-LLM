from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.price") > 930)
    .select(
        F.col("products.category"),
        F.col("inventory.product_id"),
        F.date_format(F.col("products.launch_date"), "MMM yyyy").alias("launch_date_label"),
    )
    .orderBy(F.col("product_id").desc(), F.col("launch_date_label").desc())
)
