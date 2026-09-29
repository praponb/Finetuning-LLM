from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("country", "name", "city")
    .orderBy("country", F.col("city").desc())
)
