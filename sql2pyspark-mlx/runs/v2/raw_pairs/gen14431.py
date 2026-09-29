from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg((F.max(F.col("p.price")) - F.min(F.col("p.price"))).alias("price_range"))
    .orderBy("price_range", F.col("country").desc())
)
