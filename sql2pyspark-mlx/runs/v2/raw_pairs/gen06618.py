from pyspark.sql import functions as F

result = (
    df_customers
    .select("email")
    .unionByName(df_employees.select("email"))
    .distinct()
    .orderBy("email")
)
