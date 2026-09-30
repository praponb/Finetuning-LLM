from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("supplier_id").isNull()).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.country") != "TH")
    .select(F.col("s.id"), F.col("s.rating"), F.col("s.supplier_name"), F.col("s.city"))
    .orderBy(F.col("supplier_name").desc(), F.col("id").desc())
    .limit(20)
)
