from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNull() & F.col("cost").between(240, 280))
    .withColumn("launch_date_plus_months", F.add_months("launch_date", 1))
    .withColumn("launch_date_year", F.year("launch_date"))
)
