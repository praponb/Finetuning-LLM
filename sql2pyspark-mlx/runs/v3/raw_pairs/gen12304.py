from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.level").isNotNull() & F.col("e.is_remote"))
    .select(
        F.col("d.location"),
        F.col("e.job_title"),
        F.when(F.col("e.is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy("job_title", "location")
)
