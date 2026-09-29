from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "city", "name", "email")
    .orderBy("city", "id")
    .limit(10)
)
