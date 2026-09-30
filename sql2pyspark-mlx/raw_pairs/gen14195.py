from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Umbrella", "Initech"])
    .agg(F.count("cost"))
    .orderBy("category")
)
