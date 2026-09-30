from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@gmail.com"))
    .select("email")
    .intersect(df_employees.filter(F.col("salary") >= 58000).select("email"))
)
