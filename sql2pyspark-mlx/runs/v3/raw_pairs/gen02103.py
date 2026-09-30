from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Initech", "Umbrella", "Stark"])
    .agg(F.avg("price"))
    .orderBy("category")
)
