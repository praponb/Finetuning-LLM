from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
    .filter(F.col("unique_stores") > 2)
)
