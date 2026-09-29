from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "city", "age", F.upper("city").alias("city_upper"))
    .orderBy("city_upper")
)
