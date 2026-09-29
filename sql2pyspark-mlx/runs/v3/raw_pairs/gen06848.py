from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") > 7)
    .select("category")
    .intersect(df_products.filter(F.col("brand") == "Initech").select("category"))
)
