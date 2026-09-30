from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 5) | F.col("manager_id").isin([9, 26, 32, 40]))
)
