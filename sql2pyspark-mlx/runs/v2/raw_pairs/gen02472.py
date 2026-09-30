from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Books", "Electronics"])
    .agg(F.sum("cost"))
    .orderBy("brand")
)
