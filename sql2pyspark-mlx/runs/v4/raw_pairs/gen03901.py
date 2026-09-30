from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") > 5)
    .select("category")
    .distinct()
)
