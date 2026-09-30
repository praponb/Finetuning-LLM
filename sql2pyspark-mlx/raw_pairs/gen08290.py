from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title").isin(["Manager", "Engineer"]) & (F.col("salary") < 120000)) | (F.col("dept_id") == 36))
    .select(
        "id",
        "hire_date",
        "level",
        "salary",
        F.upper("full_name").alias("full_name_upper"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
