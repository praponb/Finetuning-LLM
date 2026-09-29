from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("rating") >= 3.0).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.cost") <= 440)
    .select(F.col("p.id"), F.col("p.product_name"))
)
