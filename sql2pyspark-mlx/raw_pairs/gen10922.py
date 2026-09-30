from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([20, 32, 35]) & F.col("email").isNull() & F.col("job_title").isin(["Manager", "Intern", "Analyst"]))
    .groupBy(
        "job_title",
        F.year("hire_date").alias("hire_year"),
        F.date_trunc("MONTH", "hire_date").alias("hire_month"),
    )
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .filter(F.col("avg_salary") > 134000)
)
