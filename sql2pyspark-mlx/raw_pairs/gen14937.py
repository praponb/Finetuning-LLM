from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([5, 22]) | F.col("hire_date").between("2023-08-01", "2023-10-01"))
    .select(
        "job_title",
        "is_remote",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy("job_title")
)
