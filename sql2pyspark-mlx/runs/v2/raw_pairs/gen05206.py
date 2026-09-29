from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([12, 28, 34]))
    .select("id", "salary", "hire_date", F.round("salary", 1).alias("salary_rounded"))
)
