from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 8600)
    .select("email")
    .subtract(df_employees.select("email"))
    .orderBy("email")
)
