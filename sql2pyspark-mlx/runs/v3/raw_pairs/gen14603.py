from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country").isin(["SG", "TH", "US"]) & (F.col("s.rating") <= 2.7))
    .select(F.col("s.rating"), F.col("p.price"), F.col("p.product_name"))
)
