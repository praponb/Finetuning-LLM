from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Sports", "Electronics", "Grocery", "Books"])
    .agg(F.max("cost"))
)
