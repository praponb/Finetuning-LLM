from pyspark.sql import functions as F

result = (
    df_customers
    .select("email")
    .unionByName(df_employees.filter(F.col("is_remote")).select("email"))
)
