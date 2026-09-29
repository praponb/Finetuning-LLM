from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(710, 730))
    .groupBy("category")
    .pivot("brand", ["Umbrella", "Stark", "Initech"])
    .agg(F.sum("cost"))
    .orderBy("category")
)
