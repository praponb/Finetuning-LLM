from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "age", "city", "email")
    .orderBy("email", "id")
    .limit(10)
)
