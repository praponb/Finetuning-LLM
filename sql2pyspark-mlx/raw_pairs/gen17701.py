from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%ENR%"))
    .groupBy("dept_id", "level")
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
    )
)
