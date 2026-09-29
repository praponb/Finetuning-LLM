from pyspark.sql import functions as F

with_dims = (
    df_employees
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.budget"), F.col("b.dept_name"), F.col("b.location"))
)

with_dims_employees = (
    with_dims
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "src.id",
        "src.email",
        "src.manager_id",
        "src.job_title",
        "src.salary",
        "src.is_remote",
        "src.level",
        "src.budget",
        "src.dept_name",
        "src.location",
        F.col("d.email").alias("employee_email"),
        F.col("d.hire_date").alias("employee_hire_date"),
        F.col("d.level").alias("employee_level"),
    )
)

with_dims_employees_2 = (
    with_dims_employees
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.manager_id",
        "a.salary",
        "a.is_remote",
        "a.location",
        "a.employee_email",
        "a.employee_hire_date",
        F.col("d.full_name"),
        F.col("d.level").alias("employee_level_2"),
    )
)

with_dims_employees_3 = (
    with_dims_employees_2
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id").eqNullSafe(F.col("e.id")), "left")
    .select(
        "a.*",
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.job_title"),
    )
)

with_dims_employees_4 = (
    with_dims_employees_3
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.manager_id",
        "a.salary",
        "a.employee_email",
        "a.employee_hire_date",
        "a.employee_level_2",
        "a.employee_full_name",
        "a.employee_salary",
        "a.job_title",
        F.col("b.level"),
        F.col("b.job_title").alias("employee_job_title"),
        F.col("b.hire_date"),
    )
)

with_dims_employees_5 = (
    with_dims_employees_4
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "left")
    .select("e.*", F.col("p2.full_name"), F.col("p2.email"))
)

result = (
    with_dims_employees_5
    .groupBy("employee_job_title", "level")
    .pivot("job_title", ["Analyst", "Engineer"])
    .agg(F.max("salary").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "employee_job_title",
        "level",
        F.col("Analyst_value_sum").alias("analyst_value_sum"),
        F.col("Analyst_item_count").alias("analyst_item_count"),
        F.col("Engineer_value_sum").alias("engineer_value_sum"),
        F.col("Engineer_item_count").alias("engineer_item_count"),
    )
)
