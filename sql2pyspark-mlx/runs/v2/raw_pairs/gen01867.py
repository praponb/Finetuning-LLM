from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Grocery", "Home", "Books"])
    .agg(F.avg("price"))
    .orderBy("brand")
)
