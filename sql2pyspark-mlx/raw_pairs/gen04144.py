from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.max(F.col("p.stock_qty")).alias("max_stock_qty"),
    )
    .filter(F.col("max_stock_qty") > 220)
    .orderBy(F.col("max_stock_qty").desc(), "country")
    .limit(10)
)
