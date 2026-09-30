from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "manager_id").orderBy("salary", "id")
w2 = Window.orderBy(F.col("salary").desc())
w3 = Window.partitionBy("level")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-02-22").cast("date")) & F.month("hire_date").isin([5, 9, 10]))
    .withColumn("full_name_title", F.initcap(F.trim("full_name")))
    .withColumn("level_upper", F.upper("level"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("DIRECT")))
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    deduped
    .select("manager_id", "level")
    .exceptAll(
        df_employees
        .filter(F.col("job_title").isin(["Director", "Engineer"]))
        .select("manager_id", "level")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "inner")
    .filter(F.col("b.dept_name").isin(["Legal", "Marketing"]))
    .select("e.*", F.col("b.location"))
)

with_windows = (
    with_dims
    .withColumn("salary_rank", F.rank().over(w2))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w3)), 4))
)

valid = (
    with_windows
    .filter((F.col("hire_date") >= F.lit("2023-05-01").cast("date")) & F.col("level").isNotNull())
    .withColumn("salary_rank_per_salary", F.round((F.col("salary_rank") / F.col("salary")), 2))
    .withColumn("email_lower", F.lower("email"))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("UNKNOWN")))
)

kept_keys = (
    valid
    .select("dept_id", "job_title")
    .subtract(
        df_employees
        .filter(~F.col("job_title").isin(["Analyst", "Designer"]))
        .select("dept_id", "job_title")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "job_title_clean",
        "salary_rank",
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("dept_id").desc())
)
