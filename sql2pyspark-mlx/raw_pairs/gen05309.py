from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-08-30")))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.concat("level", F.lit("/"), "full_name").alias("level_full_name"),
        F.date_trunc("YEAR", "hire_date").alias("hire_date_period"),
    )
)

result = (
    base
    .withColumn("year_start", F.date_trunc("YEAR", "hire_date_period"))
    .withColumn("week_start", F.date_trunc("WEEK", "hire_date_period"))
    .cube("manager_id", "year_start", "week_start")
    .agg(
        F.avg(F.when(F.col("level") == "L3", F.col("salary"))).alias("l3_avg_salary"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.countDistinct("job_title").alias("distinct_job_titles_2"),
        F.grouping("manager_id").alias("grouping_level"),
    )
)
