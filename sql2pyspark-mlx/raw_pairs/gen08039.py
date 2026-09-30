from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") > "2023-04-01")
    .withColumn("sorted_tags", F.sort_array("tags"))
    .withColumn("launch_date_minus_86d", F.date_sub("launch_date", 86))
)
