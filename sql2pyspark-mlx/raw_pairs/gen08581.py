from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Initech", "Stark", "Umbrella", "Globex"])
    .agg(F.sum("cost"))
    .orderBy("category")
)
