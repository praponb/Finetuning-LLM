from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.stock_qty").isNotNull())
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(F.count(F.col("p.id")).alias("product_count"))
    .orderBy(F.col("id").desc(), "country")
    .limit(3)
)
