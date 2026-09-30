from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") > 2)
    .groupBy("category")
    .pivot("brand", ["Initech", "Globex", "Acme", "Stark"])
    .agg(F.avg("price"))
)
