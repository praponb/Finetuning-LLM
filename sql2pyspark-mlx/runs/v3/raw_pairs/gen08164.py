from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "level",
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0))) / F.count("*")), 1).alias("pct_director"),
    )
)
