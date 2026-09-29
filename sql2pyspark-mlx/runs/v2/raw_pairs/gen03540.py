from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id") == 32) & (F.col("brand") == "Umbrella"))
    .withColumn("price_whole", F.ceil("price"))
)
