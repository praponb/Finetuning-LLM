from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance") | (F.col("category") == "Home"))
    .withColumn("category_title", F.initcap(F.trim("category")))
)
