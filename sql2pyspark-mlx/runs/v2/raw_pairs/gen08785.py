from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.cost") > 260)
    .groupBy(F.col("s.city"))
    .agg(F.min(F.col("p.cost")).alias("min_cost"))
    .filter(F.col("min_cost") >= 250)
    .orderBy("min_cost")
)
