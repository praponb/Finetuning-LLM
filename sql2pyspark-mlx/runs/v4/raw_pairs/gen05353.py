from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") < 137000)
    .select("full_name")
    .union(df_customers.select("name"))
)
