from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.length("supplier_name") >= 6).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.brand").isin(["Stark", "Umbrella"]))
    .select(F.col("p.id"), F.col("p.product_name"), F.col("p.cost"), F.col("p.brand"))
)
