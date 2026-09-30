from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([5, 8, 21, 28]))
    .select("full_name", "hire_date", "level")
    .orderBy("level")
)
