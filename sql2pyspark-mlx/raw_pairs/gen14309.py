from pyspark.sql import functions as F

by_dimension = (
    df_employees
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .groupingSets(
        [["dept_name", "location", "job_title"], ["dept_name"]],
        F.col("d.dept_name").alias("dept_name"),
        F.col("d.location").alias("location"),
        F.col("a.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.salary")).alias("total_salary"),
    )
)

result = (
    by_dimension
    .select("dept_name", "location", "total_salary")
    .orderBy(F.col("location").desc())
)
