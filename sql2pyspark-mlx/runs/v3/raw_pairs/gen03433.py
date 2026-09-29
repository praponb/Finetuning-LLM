from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L1")
    .select("id", "dept_id", "manager_id", "full_name")
    .orderBy(F.col("dept_id").desc())
)
