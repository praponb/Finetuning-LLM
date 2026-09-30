from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.count("*").alias("order_item_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.order_item_count") >= 7)
    .select(
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("pt.unique_orders"),
        F.col("pt.order_item_count"),
    )
)
