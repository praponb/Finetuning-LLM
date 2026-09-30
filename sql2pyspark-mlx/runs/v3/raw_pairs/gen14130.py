from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([9, 12, 14]))
    .withColumn("product_name_category", F.concat("product_name", F.lit(" "), "category"))
    .withColumn("is_home", F.when(F.col("category") == "Home", 1).otherwise(0))
)
