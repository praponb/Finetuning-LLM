from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([1, 4, 12]) | (F.col("job_title") != "Analyst"))
)
