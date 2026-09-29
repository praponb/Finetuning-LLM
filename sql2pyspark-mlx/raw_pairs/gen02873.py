from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "cost")
    .orderBy(F.col("id").desc())
    .limit(5)
)
