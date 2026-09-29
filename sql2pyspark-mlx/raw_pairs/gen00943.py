from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .subtract(df_employees.select("email"))
    .orderBy(F.col("country").desc())
)
