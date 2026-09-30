from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("launch_date_period", F.date_trunc("week", "launch_date"))
    .withColumn("category_title", F.initcap(F.trim("category")))
)
