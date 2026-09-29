from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        "manager_id",
        "dept_id",
    )
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.count_if(F.col("level") == "L1").alias("l1_events"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L1", 1)), F.lit(0)), 2).alias("salary_per_l1"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.avg(F.when(F.col("level") == "L3", F.col("salary"))).alias("l3_avg_salary"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
    )
    .orderBy("l1_events")
)
