from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Initech")
    .select("category")
    .distinct()
    .orderBy("category")
)
