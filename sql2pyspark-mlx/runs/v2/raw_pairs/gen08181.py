from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Stark", "Acme", "Globex", "Initech"])
    .agg(F.avg("cost"))
    .orderBy("category")
)
