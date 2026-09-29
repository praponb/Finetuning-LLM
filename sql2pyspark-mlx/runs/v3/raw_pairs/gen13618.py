from pyspark.sql import functions as F

result = (
    df_products
    .select("brand", "category")
    .distinct()
)
