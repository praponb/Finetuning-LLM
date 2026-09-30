from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Sports", "Books", "Electronics"])
    .agg(F.avg("cost"))
    .orderBy("brand")
)
