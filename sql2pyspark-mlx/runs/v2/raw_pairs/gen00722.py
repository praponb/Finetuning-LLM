from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("city").isin(["Chiang Mai", "Berlin", "Tokyo"])).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.price"), F.col("p.cost"))
    .orderBy(F.col("id").desc())
)
