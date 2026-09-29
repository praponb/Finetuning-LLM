from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter((F.col("hire_date") < "2024-04-08") | (F.col("job_title") == "Designer"))
)

summary = (
    filtered_employees
    .groupBy("manager_id")
    .agg(
        F.max("salary").alias("max_salary"),
        F.max("hire_date").alias("latest_hire_date"),
    )
)

result = (
    summary
    .filter(F.col("max_salary") >= 88000)
    .select("manager_id", "max_salary", "latest_hire_date")
    .orderBy("max_salary")
)
