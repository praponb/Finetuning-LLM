from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.sum(F.col("p.cost")).alias("total_cost"))
    .filter(F.col("total_cost") >= 3100)
)
