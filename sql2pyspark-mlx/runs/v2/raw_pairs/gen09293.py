from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("brand").isin(["Stark", "Umbrella", "Acme"])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.country").isin(["UK", "SG", "US"]))
    .select(F.col("s.id"), F.col("s.country"))
)
