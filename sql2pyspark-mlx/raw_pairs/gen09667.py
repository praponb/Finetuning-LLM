from pyspark.sql import functions as F

result = (
    df_employees
    .cube("job_title", "dept_id", "manager_id")
    .agg(
        F.stddev_samp("salary").alias("salary_stddev"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.grouping("job_title").alias("grouping_id"),
    )
    .orderBy("manager_id")
)
