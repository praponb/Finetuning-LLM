from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2023-11-01")
    .select("hire_date")
    .unionByName(df_employees.filter(F.col("is_remote")).select("hire_date"))
    .distinct()
    .orderBy(F.col("hire_date").desc())
)
