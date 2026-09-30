from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Acme", "Umbrella", "Initech", "Globex"])
    .agg(F.sum("price"))
    .orderBy("category")
)
