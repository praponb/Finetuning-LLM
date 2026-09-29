from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("category") != "Fashion").alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(~F.col("s.country").isin(["UK", "SG", "JP"]))
    .select(F.col("s.id"), F.col("s.rating"), F.col("s.country"))
)
