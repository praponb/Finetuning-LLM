from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.col("s.city").isin(["New York", "Bangkok"]) & (F.col("s.country") != "UK"))
    .select(F.col("p.id"), F.col("p.price"), F.col("s.city"))
)
