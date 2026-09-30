from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([1, 3, 9]))
    .select(
        "email",
        "dept_id",
        F.when(F.col("job_title").isin(["Designer", "Engineer"]), "Priority").otherwise("Other").alias("job_title_group"),
    )
)
