from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 37)
    .select("name", "segment", "signup_date")
    .orderBy(F.col("segment").desc(), F.col("name").desc())
)
