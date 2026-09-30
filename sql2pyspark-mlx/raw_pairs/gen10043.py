from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-10").cast("date")) & F.col("level").isin(["L2", "L4", "L5"]))
    .withColumn("salary_rounded", F.round("salary", 2))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.dept_id",
        "a.manager_id",
        "a.job_title",
        "a.salary_rounded",
        F.col("p.dept_name"),
        F.col("p.budget"),
    )
)

cleaned_employees = (
    with_dims
    .filter(F.col("dept_name").isin(["Finance", "HR"]))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary_rounded",
        "dept_name",
        "budget",
        F.concat_ws("-", "dept_name", "job_title").alias("dept_name_job_title_key"),
        F.trim("dept_name").alias("clean_dept_name"),
    )
)

base = (
    cleaned_employees
    .filter(~F.col("job_title").eqNullSafe(F.lit("Designer")))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

joined = (
    base
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_id").isin([13, 19, 38]))
    .select("a.id", "a.dept_id", "a.manager_id", "a.dept_name", F.col("d.salary"))
)

base_employees = (
    joined
    .filter(F.col("dept_name").isNotNull())
    .withColumn("salary_rounded", F.round("salary", 2))
)

result = (
    base_employees
    .alias("e")
    .join(df_employees.alias("dim"), F.col("e.manager_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["job_title", "level"], ["job_title"]],
        F.col("dim.job_title").alias("job_title"),
        F.col("dim.level").alias("level"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("e.salary_rounded")), 2).alias("avg_salary_rounded"),
        F.avg(F.col("e.salary")).alias("avg_salary"),
        F.grouping_id().alias("grouping_level"),
    )
)
