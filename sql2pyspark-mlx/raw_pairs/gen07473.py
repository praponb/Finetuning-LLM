from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(F.col("supplier_name").alias("label"))
    .subtract(df_products.select(F.col("category").alias("label")))
    .orderBy("label")
)
