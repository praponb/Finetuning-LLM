from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(34, 38))
    .select("email")
    .intersect(df_employees.filter(F.col("manager_id").isNull()).select("email"))
)
