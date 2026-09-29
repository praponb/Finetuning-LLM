from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 321, "high").when(F.col("p.stock_qty") >= 160, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
