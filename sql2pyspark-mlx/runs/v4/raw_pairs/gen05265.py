from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.when(F.col("category").isin(["Home", "Grocery"]), "group_a").otherwise("other").alias("category_group"))
    .pivot("brand", ["Initech", "Globex", "Umbrella", "Stark"])
    .agg(F.count("price"))
    .orderBy("category_group")
)
