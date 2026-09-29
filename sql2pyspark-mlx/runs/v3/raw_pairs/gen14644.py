from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Sports", "Home"])
    .agg(F.count("cost"))
    .orderBy("brand")
)
