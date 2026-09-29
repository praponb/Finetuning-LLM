from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.supplier_id").isNull())
    .groupBy(
        F.col("products.category"),
        F.date_trunc("MONTH", F.col("products.launch_date")).alias("launch_month"),
    )
    .agg(F.countDistinct(F.col("order_items.unit_price")).alias("unique_products"))
    .orderBy("category", F.col("unique_products").desc())
)
