from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.launch_date") <= "2023-11-01")
    .groupBy(F.col("products.supplier_id"), F.col("products.brand"))
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("num_order_items"),
    )
)
