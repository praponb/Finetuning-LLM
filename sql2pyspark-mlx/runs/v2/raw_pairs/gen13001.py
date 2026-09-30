from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Books", "Electronics", "Sports", "Fashion"])
    .agg(F.avg("cost"))
    .orderBy("brand")
)
