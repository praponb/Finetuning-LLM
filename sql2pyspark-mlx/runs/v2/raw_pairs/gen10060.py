from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper("email").like("%ENR%") & F.col("email").like("%@gmail.com"))
    .select("id", "country", F.length("city").alias("city_length"))
    .orderBy("id", "city_length")
)
