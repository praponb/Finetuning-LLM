from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([1, 10, 16]))
    .select("id", "level", "full_name", "hire_date")
)
