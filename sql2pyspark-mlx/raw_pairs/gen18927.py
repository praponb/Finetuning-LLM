from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Grocery")
    .withColumn("price_rounded", F.round("price", 0))
    .orderBy("launch_date")
)
