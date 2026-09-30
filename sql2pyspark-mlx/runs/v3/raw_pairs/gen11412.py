from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(~F.col("p.brand").isin(["Stark", "Initech", "Umbrella"]))
    .select(F.col("s.country"), F.col("p.category"), F.col("p.supplier_id"))
)
