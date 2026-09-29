from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("manager_id").isin([22, 26, 40]) & F.col("full_name").like("%Wong%"))
)

hire_cohort_summary = (
    base
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    hire_cohort_summary
    .select("hire_cohort", "job_title_list", "avg_salary")
    .orderBy("hire_cohort")
)
