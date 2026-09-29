from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.cost").between(260, 290))
    .groupBy(F.col("s.city"))
    .agg(F.sum(F.col("p.cost")).alias("total_cost"))
    .orderBy("city", "total_cost")
)
