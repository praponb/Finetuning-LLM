from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("p.supplier_id"),
        F.when(F.col("p.stock_qty") >= 302, "high").when(F.col("p.stock_qty") >= 81, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.countDistinct(F.col("ss.revenue")).alias("unique_products"))
    .orderBy(F.col("stock_qty_bucket").desc(), "supplier_id")
    .limit(3)
)
