from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.upper(F.trim(F.col("p.product_name"))) == "NOISE-CANCEL HEADPHONES")
    .select(F.col("s.city"), F.col("s.supplier_name"), F.col("p.category"))
)
