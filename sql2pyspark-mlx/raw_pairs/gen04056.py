from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.filter(F.col("city") == "Berlin").alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left_semi")
    .select(F.col("products.id"), F.col("products.supplier_id"))
    .orderBy("supplier_id", "id")
    .limit(10)
)
