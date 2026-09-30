from pyspark.sql import functions as F

with_dims = (
    df_employees
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.budget"))
)

valid = (
    with_dims
    .filter(F.col("manager_id").isNotNull())
    .withColumn("full_name_level", F.concat("full_name", F.lit(" "), "level"))
)

cleaned = (
    valid
    .filter(F.col("job_title").isin(["Director", "Intern"]))
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "budget",
        "full_name_level",
        F.concat_ws("/", "full_name", "job_title").alias("full_name_job_title_key"),
    )
)

remaining_keys = (
    cleaned
    .select("id", "level")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isin([32, 37]))
        .select("id", "level")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    verified
    .select("level", "id")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([1, 9, 18, 23]))
        .select("level", "id")
    )
)

verified_employees = (
    verified
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    verified_employees
    .groupBy(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_rows"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
    )
)

result = totals.select("quarter_start", "manager_rows", "avg_salary", "salary_quartiles")
