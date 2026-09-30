from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("supplier_id").isin([9, 15, 34])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("s.rating"), F.col("s.city"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
