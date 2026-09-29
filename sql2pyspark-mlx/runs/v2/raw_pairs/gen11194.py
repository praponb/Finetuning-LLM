from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select("email", "level")
    .unionByName(df_employees.filter(F.col("dept_id") == 10).select("email", "level"))
    .distinct()
)
