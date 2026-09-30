from pyspark.sql import functions as F

result = (
    df_customers
    .select("email")
    .subtract(df_employees.filter(F.col("is_remote")).select("email"))
    .orderBy(F.col("email").desc())
)
