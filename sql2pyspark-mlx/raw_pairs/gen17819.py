from pyspark.sql import functions as F

result = (
    df_employees
    .alias("src")
    .join(df_departments.alias("dim"), F.col("src.dept_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["location", "job_title"], ["location"], []],
        F.col("dim.location").alias("location"),
        F.col("src.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.salary")).alias("max_salary"),
    )
)
