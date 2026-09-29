from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.cost").isNull())
    .groupBy(F.col("s.country"))
    .agg(F.max(F.col("p.stock_qty")).alias("max_stock_qty"))
    .orderBy(F.col("country").desc())
)
