from pyspark.sql import functions as F

result = (
    df_products
    .select("category")
    .distinct()
    .orderBy("category")
    .limit(20)
)
