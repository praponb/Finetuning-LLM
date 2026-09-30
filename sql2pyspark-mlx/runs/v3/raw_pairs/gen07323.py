from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter((F.col("p.price") >= 830) & (F.col("p.category") == "Electronics"))
    .select(
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("oi.unit_price"),
        F.substring(F.col("p.brand"), 1, 2).alias("brand_prefix"),
    )
)
