from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 10400)
    .select("email")
    .unionByName(df_employees.select("email"))
    .orderBy("email")
)
