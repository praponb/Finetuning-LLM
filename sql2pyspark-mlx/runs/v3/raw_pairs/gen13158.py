from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 446, "high").when(F.col("p.stock_qty") >= 107, "mid").otherwise("low").alias("stock_qty_bucket"),
        F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"),
    )
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
)
