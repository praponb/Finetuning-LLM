from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "job_title",
        "full_name",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy("is_remote_label", "id")
    .limit(20)
)
