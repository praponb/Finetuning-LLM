from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.category") == "Home")
    .groupBy(F.col("s.country"))
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("cnt").desc())
)
