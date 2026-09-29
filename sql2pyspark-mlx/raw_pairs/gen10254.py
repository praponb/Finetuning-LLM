from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 1)
    .select(
        "dept_id",
        "hire_date",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
    )
    .orderBy("dept_id")
)
