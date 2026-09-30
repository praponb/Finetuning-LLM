from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Emm%") | (F.col("city") == "London"))
    .select("age", F.lower("city").alias("city_lower"))
    .orderBy(F.col("age").desc())
)
