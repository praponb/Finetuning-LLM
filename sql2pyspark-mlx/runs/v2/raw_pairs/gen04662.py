from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([33, 37]))
    .select("brand")
    .distinct()
    .orderBy(F.col("brand").desc())
    .limit(10)
)
