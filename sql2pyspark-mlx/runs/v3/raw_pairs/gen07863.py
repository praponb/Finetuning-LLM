from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("city").isin(["Chiang Mai", "Singapore"])).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.brand") == "Initech")
    .select(F.col("p.id"), F.col("p.product_name"), F.col("p.cost"))
)
