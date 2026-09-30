from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%LIC%") & F.col("hire_date").between("2023-05-20", "2023-11-01"))
    .groupBy(
        "job_title",
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
        F.max("hire_date").alias("latest_hire_date"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)
