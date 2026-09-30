from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select("email")
    .unionByName(df_employees.filter(F.col("salary") >= 147000).select("email"))
    .distinct()
)
