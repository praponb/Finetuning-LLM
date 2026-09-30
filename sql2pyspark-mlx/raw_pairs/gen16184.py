from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("level", "dept_id")
    .subtract(
        df_employees
        .filter(F.col("level") != "L3")
        .select("level", "dept_id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("job_title", F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_rows"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L5", 1)), F.lit(0)), 2).alias("salary_per_l5"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Intern", 1)), F.lit(0)), 2).alias("salary_per_intern"),
        F.stddev_samp("salary").alias("salary_stddev"),
    )
)
