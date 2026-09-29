from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.order_id").isin([18, 35]) & (F.col("p.price") <= 420))
    .select(
        F.col("p.category"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.floor(F.col("p.cost")).alias("cost_whole"),
    )
)
