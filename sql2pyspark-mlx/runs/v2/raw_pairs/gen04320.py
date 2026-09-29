from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower(F.trim("email")) == "liam@outlook.com")
    .select("id", "country", F.lower("name").alias("name_lower"))
    .orderBy(F.col("country").desc(), "id")
    .limit(20)
)
