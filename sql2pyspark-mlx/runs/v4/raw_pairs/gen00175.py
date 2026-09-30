from pyspark.sql import functions as F

base = df_employees.filter(F.col("manager_id").isNotNull() & (F.col("salary") >= 95000))

is_remote_summary = (
    base
    .groupBy("is_remote")
    .agg(F.min("hire_date").alias("first_hire_date"))
)

result = is_remote_summary.select("is_remote", "first_hire_date")
