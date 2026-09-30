from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Initech", "Globex"])
    .agg(F.max("price"))
    .orderBy("category")
)
