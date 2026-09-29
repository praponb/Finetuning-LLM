from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.supplier_id") == 36)
    .groupBy(F.col("s.city"))
    .agg(F.count("*").alias("num_products"))
)
