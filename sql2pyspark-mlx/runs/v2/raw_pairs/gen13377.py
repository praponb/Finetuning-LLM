from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isNull())
    .select("email")
    .subtract(df_employees.select("email"))
    .orderBy("email")
)
