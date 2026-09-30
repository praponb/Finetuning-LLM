from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 3)
    .select("dept_id")
    .orderBy(F.col("dept_id").desc())
)
