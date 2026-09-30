from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("salary") <= 65000)
    .select("*", F.col("is_remote").cast("int").alias("is_remote_flag"))
)

result = (
    filtered
    .alias("a")
    .join(df_departments.alias("dim"), F.col("a.dept_id") == F.col("dim.id"), "inner")
    .cube(
        F.col("dim.location").alias("location"),
        F.col("dim.dept_name").alias("dept_name"),
        F.col("a.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.is_remote_flag")).alias("min_is_remote_flag"),
        F.max(F.col("a.salary")).alias("max_salary"),
    )
)
