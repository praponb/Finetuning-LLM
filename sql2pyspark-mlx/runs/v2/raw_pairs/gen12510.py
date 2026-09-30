from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.year("hire_date") >= 2023) & (F.col("manager_id") == 29))
    .select("id", "is_remote")
)
