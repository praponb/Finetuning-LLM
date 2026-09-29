from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("id", "country", "age")
    .orderBy(F.col("country").desc(), F.col("age").desc(), "id")
    .limit(10)
)
