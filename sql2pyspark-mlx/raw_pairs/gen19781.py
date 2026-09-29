from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Manager", "Intern"]) & (F.col("dept_id") == 39) & F.col("full_name").isNotNull())
    .groupBy(
        "job_title",
        "level",
        F.date_format("hire_date", "yyyy-MM").alias("hire_month"),
    )
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") < 101000)
    .orderBy("avg_salary")
)
