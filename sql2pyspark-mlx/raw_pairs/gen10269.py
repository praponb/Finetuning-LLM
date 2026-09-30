from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title").orderBy("hire_date", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

filtered = (
    df_employees
    .filter(F.col("hire_date").between("2023-11-14", "2025-01-25"))
    .select("*", F.col("salary").cast("int").alias("salary_int"))
)

scored = (
    filtered
    .select(
        "*",
        F.lag("salary_int").over(w).alias("prev_salary_int"),
        F.lead("salary_int").over(w).alias("next_salary_int"),
        F.lag("salary").over(w).alias("prev_salary"),
        F.lead("salary").over(w).alias("next_salary"),
        F.row_number().over(w).alias("seq_no"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "left")
    .filter(~F.col("d.level").isin(["L1", "L2"]))
    .select(
        "a.*",
        F.col("d.full_name").alias("employee_full_name"),
        F.col("d.email").alias("employee_email"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.email",
        "a.dept_id",
        "a.manager_id",
        "a.hire_date",
        "a.prev_salary_int",
        "a.next_salary_int",
        "a.prev_salary",
        "a.next_salary",
        "a.seq_no",
        "a.employee_email",
        F.col("e.full_name").alias("employee_full_name_2"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.email").alias("employee_email_2"),
    )
)

filtered_employees = (
    with_dims
    .filter((F.col("hire_date") > F.lit("2023-04-09").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "prev_salary_int",
        "next_salary_int",
        "next_salary",
        "seq_no",
        "employee_email",
        "employee_full_name_2",
        "employee_job_title",
        "employee_email_2",
        F.upper("employee_job_title").alias("employee_job_title_upper"),
    )
)

result = (
    filtered_employees
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "email",
        "dept_id",
        "hire_date",
        "prev_salary_int",
        "next_salary",
        "seq_no",
        "employee_email",
        "employee_full_name_2",
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("next_salary").desc_nulls_first())
)
