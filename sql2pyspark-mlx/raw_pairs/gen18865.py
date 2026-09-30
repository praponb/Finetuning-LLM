from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-05-31").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        F.trim("full_name").alias("clean_full_name"),
    )
)

result = (
    valid
    .groupBy("manager_id")
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.count_if(F.col("job_title") == "Director").alias("director_events"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
        F.size(F.collect_set("job_title")).alias("num_job_titles_2"),
    )
    .orderBy("manager_id")
)
