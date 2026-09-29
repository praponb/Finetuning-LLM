from pyspark.sql import functions as F

src = df_employees.filter((F.col("level") == "L2") & F.col("email").isNull())

job_title_summary = (
    src
    .groupBy("job_title")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
    )
)

result = (
    job_title_summary
    .filter(F.col("l4_salary") > 36)
    .orderBy("job_title", "first_hire_date")
)
