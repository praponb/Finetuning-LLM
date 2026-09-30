from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") < "2025-08-01")
    .select("name")
    .unionByName(df_customers.filter(F.col("age").between(35, 44)).select("name"))
    .distinct()
)
