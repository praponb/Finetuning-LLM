from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"))
    .filter(F.col("umbrella_count") >= 5)
    .select("brand", "umbrella_count")
)
