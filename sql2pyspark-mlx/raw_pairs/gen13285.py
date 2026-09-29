from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")

enriched = (
    df_employees
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.location"), F.col("d.budget"))
)

joined = (
    enriched
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "inner")
    .select(
        "a.*",
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.email").alias("employee_email"),
    )
)

joined_employees = (
    joined
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "src.*",
        F.col("d.email").alias("employee_email_2"),
        F.col("d.salary").alias("employee_salary"),
        F.col("d.hire_date").alias("employee_hire_date_2"),
    )
)

latest = (
    joined_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "employee_job_title",
        F.col("budget").alias("BUDGET_VAL"),
        F.col("salary").alias("SALARY_VAL"),
    )
    .unpivot(
        ids=["id", "employee_job_title"],
        values=["BUDGET_VAL", "SALARY_VAL"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
