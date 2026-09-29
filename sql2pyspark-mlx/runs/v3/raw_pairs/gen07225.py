from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Manager"]) & (F.col("manager_id").isin([9, 34]) | F.col("is_remote")))
    .select(
        "is_remote",
        "manager_id",
        F.date_add("hire_date", 41).alias("hire_date_plus_41d"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
