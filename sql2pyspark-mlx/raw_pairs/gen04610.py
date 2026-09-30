from pyspark.sql import functions as F

min_age = df_customers.filter(F.col("credit_limit").between(8600, 13600)).agg(F.min("age")).first()[0]

result = (
    df_customers
    .filter(F.col("age") == min_age)
    .select("id", "age", "email", "is_active")
    .orderBy("email", "id")
    .limit(5)
)
