from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("id", "manager_id")
    .intersect(
        df_employees
        .filter(F.upper(F.trim("email")) == "LIAM@OUTLOOK.COM")
        .select("id", "manager_id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("month", "hire_date").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary"))).alias("l5_total_salary"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.min("salary").alias("min_salary"),
        F.stddev_samp("salary").alias("salary_stddev"),
    )
    .orderBy(F.col("salary_stddev").desc())
)
