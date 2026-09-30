from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select("category")
    .distinct()
)
