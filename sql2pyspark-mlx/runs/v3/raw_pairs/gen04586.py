from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "right")
    .filter(~(F.col("oi.product_id").isNotNull() & F.col("oi.order_id").isin([25, 28, 32, 37])))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("p.brand"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.round((F.col("p.cost") * 100), 2).alias("cost_scaled"),
    )
)
