from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("dept_id")
    .intersect(
        df_employees
        .filter(F.col("dept_id") == 36)
        .select("dept_id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("job_title")
    .agg(
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_rows"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L5", 1)), F.lit(0)), 2).alias("salary_per_l5"),
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
    )
    .orderBy("intern_count")
)
