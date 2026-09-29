from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.size(F.col("p.tags")) >= 2)
    .groupBy(F.col("s.country"))
    .agg(F.countDistinct(F.col("p.cost")).alias("unique_brands"))
)
