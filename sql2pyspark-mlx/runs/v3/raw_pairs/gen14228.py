from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNotNull())
    .groupBy(F.when(F.col("brand").isin(["Acme", "Globex"]), "tier_1").otherwise("other").alias("brand_group"))
    .pivot("brand", ["Initech", "Stark", "Umbrella"])
    .agg(F.count("cost"))
    .orderBy("brand_group")
)
