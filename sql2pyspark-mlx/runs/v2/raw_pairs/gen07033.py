from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([1, 22, 37]) & ~F.col("full_name").like("%Chen%"))
    .select("id", "email", "salary")
    .orderBy(F.col("salary").desc())
)
