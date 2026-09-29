from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.1, "high").when(F.col("suppliers.rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
        F.col("suppliers.country"),
    )
    .agg(F.round(F.avg(F.col("inventory.quantity")), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 58)
    .orderBy(F.col("avg_quantity").desc())
)
