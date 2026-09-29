from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("supplier_id").isin([11, 27, 37])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.city").isin(["Berlin", "London", "Singapore"]))
    .select(F.col("s.id"), F.col("s.city"))
)
