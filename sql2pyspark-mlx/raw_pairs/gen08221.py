from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("dept_id", "level")
    .intersectAll(
        df_employees
        .filter(F.col("level") != "L1")
        .select("dept_id", "level")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("YEAR", "hire_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
    )
    .orderBy(F.col("year_start").desc())
)
