from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Jac%"))
    .select("dept_id", "level", F.month("hire_date").alias("hire_date_month"))
    .orderBy("hire_date_month")
)
