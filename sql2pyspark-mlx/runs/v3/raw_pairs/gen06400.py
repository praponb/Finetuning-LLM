from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Umbrella", "Initech", "Globex"])
    .agg(F.sum("price"))
)
