from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("stock_qty") == 135)
    .groupBy("supplier_id")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.avg("price").alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("S.stock_qty_range"), F.col("S.avg_price"))
    .orderBy(F.col("stock_qty_range").desc())
)
