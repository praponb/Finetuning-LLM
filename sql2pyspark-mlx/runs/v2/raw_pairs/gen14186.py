from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "cost", "supplier_id")
    .orderBy(F.col("supplier_id").desc())
)
