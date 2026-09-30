from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.rating").between(3.9, 4.1))
    .groupBy(F.col("s.city"), F.col("s.country"))
    .agg(F.avg(F.col("p.cost")).alias("avg_cost"))
    .filter(F.col("avg_cost") >= 680)
    .orderBy(F.col("avg_cost").desc())
)
