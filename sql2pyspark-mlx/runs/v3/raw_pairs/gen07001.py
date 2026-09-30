from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .subtract(df_customers.select("signup_date"))
    .orderBy("hire_date")
)
