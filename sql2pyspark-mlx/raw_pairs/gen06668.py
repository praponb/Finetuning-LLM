from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("manager_id")
    .subtract(
        df_employees
        .filter(F.col("is_remote"))
        .select("manager_id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.*",
        F.col("b.full_name").alias("employee_full_name"),
        F.col("b.salary").alias("employee_salary"),
        F.col("b.hire_date").alias("employee_hire_date"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "left")
    .select(
        "e.*",
        F.col("p2.level").alias("employee_level"),
        F.col("p2.salary").alias("employee_salary_2"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .filter(F.col("b.budget") < 3553000)
    .select("e.*", F.col("b.dept_name"), F.col("b.budget"))
)

joined_employees = (
    joined
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.dept_id",
        "src.manager_id",
        "src.is_remote",
        "src.employee_full_name",
        "src.employee_salary",
        "src.dept_name",
        "src.budget",
        F.col("b.job_title").alias("employee_job_title"),
    )
)

result = (
    joined_employees
    .groupBy("employee_job_title")
    .pivot("dept_name", ["HR", "Finance"])
    .agg(F.max("employee_salary").alias("value_sum"))
    .select(
        "employee_job_title",
        F.col("HR").alias("hr"),
        F.col("Finance").alias("finance"),
    )
)
