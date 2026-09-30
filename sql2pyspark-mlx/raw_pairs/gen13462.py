from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.city").isNotNull())
    .groupBy(F.col("s.country"))
    .agg(F.sum(F.col("p.stock_qty")).alias("total_stock_qty"))
    .orderBy(F.col("total_stock_qty").desc(), "country")
)
