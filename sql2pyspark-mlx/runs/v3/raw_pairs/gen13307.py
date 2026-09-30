from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 28)
    .select("brand")
    .distinct()
)
