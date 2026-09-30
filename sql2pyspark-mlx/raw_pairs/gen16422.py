from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.avg(F.col("p.stock_qty")).alias("avg_stock_qty"),
    )
    .orderBy(F.col("product_count").desc())
)
