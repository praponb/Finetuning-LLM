from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.filter(F.col("country") == "SG").alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left_semi")
    .filter(F.size(F.col("products.tags")) > 2)
    .select(F.col("products.id"), F.col("products.category"))
)
