from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("suppliers.country"))
    .agg(F.max(F.col("products.launch_date")).alias("last_launch_date"))
    .orderBy(F.col("country").desc())
)
