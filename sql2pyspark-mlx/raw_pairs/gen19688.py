from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isNotNull())
    .select("category")
    .union(df_stores.filter(F.col("city").isNotNull()).select("city"))
)
