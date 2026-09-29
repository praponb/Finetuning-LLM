from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("product_name")
    .union(df_sessions.filter(F.col("browser").isNotNull()).select("browser"))
    .distinct()
    .orderBy(F.col("product_name").desc())
)
