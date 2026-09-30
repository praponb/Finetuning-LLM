from pyspark.sql import functions as F

result = (
    df_employees
    .rollup("dept_id")
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.max(F.when(F.col("job_title") == "Director", F.col("salary"))).alias("director_max_salary"),
        F.grouping("dept_id").alias("grouping_id"),
    )
    .orderBy(F.col("dept_id").asc_nulls_last())
)
