from pyspark.sql import functions as F

result = (
    df_employees
    .select("full_name")
    .subtract(df_customers.select("name"))
    .orderBy("full_name")
)
