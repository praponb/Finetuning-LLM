from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id") == 35) & F.col("dept_id").isin([7, 10]))
    .select("email", "salary")
    .orderBy(F.col("salary").desc())
)
