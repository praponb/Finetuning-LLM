from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.count_if(F.col("level") == "L2").alias("l2_events"),
        F.avg(F.when(F.col("job_title") == "Intern", F.col("salary"))).alias("intern_avg_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)
