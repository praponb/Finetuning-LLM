from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget") <= 2304000).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .select(
        F.col("e.id"),
        F.col("e.salary"),
        F.col("e.is_remote"),
        F.col("e.job_title"),
    )
    .orderBy(F.col("id").desc(), "salary")
)
