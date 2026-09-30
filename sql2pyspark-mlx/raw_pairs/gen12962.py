from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Stark", "Umbrella", "Initech"])
    .agg(F.sum("cost").alias("total"))
)
