from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.city"))
    .agg(F.countDistinct(F.col("p.stock_qty")).alias("unique_brands"))
    .filter(F.col("unique_brands") >= 1)
)
