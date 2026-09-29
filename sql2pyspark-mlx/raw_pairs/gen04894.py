from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Books", "Home", "Sports"])
    .agg(F.sum("price"))
)
