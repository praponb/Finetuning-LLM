from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("id", "country", "age", "credit_limit")
    .orderBy("age", "id")
    .limit(3)
)
