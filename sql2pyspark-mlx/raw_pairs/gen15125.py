from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Books", "Home", "Electronics", "Sports"])
    .agg(F.max("price"))
    .orderBy("brand")
)
