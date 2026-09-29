from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-03-08", "2025-10-01") | (F.col("hire_date") < "2025-10-01"))
    .groupBy("is_remote")
    .agg(F.min("hire_date").alias("first_hire_date"))
    .orderBy("first_hire_date")
)
