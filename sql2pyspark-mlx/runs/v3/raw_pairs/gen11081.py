from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Fashion", "Sports", "Electronics", "Books"])
    .agg(F.count("cost"))
    .orderBy("brand")
)
