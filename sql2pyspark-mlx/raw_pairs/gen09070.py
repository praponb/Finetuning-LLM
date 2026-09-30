from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date_minus_5d")
w2 = Window.partitionBy(F.month("hire_date_plus_months"), "dept_id")
w3 = Window.partitionBy("dept_id", "job_title")
w4 = Window.partitionBy("level", "dept_id")
w5 = Window.partitionBy("level").orderBy(F.col("level_stddev_samp_salary").asc_nulls_last())

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-09-04").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.date_sub("hire_date", 5).alias("hire_date_minus_5d"),
        F.add_months("hire_date", 6).alias("hire_date_plus_months"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "dept_id", "job_title", "salary", "level", "hire_date_plus_months")
)

remaining_keys = (
    ranked
    .select("id", "job_title")
    .subtract(
        df_employees
        .filter(F.col("hire_date") > "2024-07-01")
        .select("id", "job_title")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(F.col("job_title") != "Intern")
    .select(
        "id",
        "job_title",
        "salary",
        "level",
        F.max("salary").over(w2).alias("hire_date_plus_months_month_max_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.stddev_samp("salary").over(w4).alias("level_stddev_samp_salary"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "job_title",
        "level_stddev_samp_salary",
        F.cume_dist().over(w5).alias("level_stddev_samp_salary_cume_dist"),
    )
)

result = (
    scored
    .groupBy("job_title")
    .agg(
        F.round(F.sum("level_stddev_samp_salary_cume_dist") / F.nullif(F.count(F.when(F.col("job_title") == "Director", 1)), F.lit(0)), 2).alias("level_stddev_samp_salary_cume_dist_per_director"),
        F.count_if(F.col("job_title") == "Engineer").alias("engineer_events"),
    )
    .orderBy("engineer_events")
)
