from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Grocery", "Home"])
    .agg(F.sum("price").alias("value_sum"))
    .select("brand", F.col("Grocery").alias("grocery"), F.col("Home").alias("home"))
)
