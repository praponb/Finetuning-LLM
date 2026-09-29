from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Stark", "Umbrella"])
    .agg(F.avg("price"))
    .orderBy("category")
)
