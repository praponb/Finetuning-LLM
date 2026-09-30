from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Fashion", "Sports", "Electronics"])
    .agg(F.max("price"))
    .orderBy("brand")
)
