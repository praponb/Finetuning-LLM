from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") != "Berlin")
    .withColumn("country_length", F.length("country"))
    .orderBy("city", "email")
)
