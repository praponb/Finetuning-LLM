from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("p.category"),
        F.when(F.col("p.stock_qty") >= 355, "high").when(F.col("p.stock_qty") >= 143, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy("category")
)
