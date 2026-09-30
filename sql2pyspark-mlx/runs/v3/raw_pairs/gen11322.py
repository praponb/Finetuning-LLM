from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") > "2024-08-01")
    .select("id", "is_remote")
)
