from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("level").isin(["L2", "L4", "L5"]) & F.col("job_title").isin(["Intern", "Director", "Engineer"]))
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.max("salary").alias("max_salary"),
    )
    .orderBy(F.col("max_salary").desc())
)
