from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.size("tags") >= 2) & (F.col("brand") != "Acme"))
    .withColumn("cost_whole", F.ceil("cost"))
    .withColumn("launch_date_plus_months", F.add_months("launch_date", 3))
)
