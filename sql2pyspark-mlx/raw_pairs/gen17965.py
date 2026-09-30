from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Umbrella", "Stark", "Initech"])
    .agg(F.sum("cost"))
    .orderBy("category")
)
