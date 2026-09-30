from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 770)
    .withColumn("price_level", F.when(F.col("price") >= 960, "High").when(F.col("price") >= 590, "Medium").otherwise("Low"))
    .withColumn("category_upper", F.upper("category"))
)
