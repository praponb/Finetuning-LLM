from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("manager_id", "level").orderBy(F.col("salary").desc(), "id")

metrics = (
    df_employees
    .withColumn("running_salary", F.sum("salary").over(w))
    .withColumn("salary_cume_dist", F.cume_dist().over(w2))
)

valid = (
    metrics
    .filter(F.col("manager_id").isNotNull())
    .select("*", F.split(F.lower("email"), "@").getItem(0).alias("email_user"))
)

with_dims = (
    valid
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "inner")
    .filter(F.col("e.dept_id").isin([15, 21, 35]))
    .select(
        "s.*",
        F.col("e.email").alias("employee_email"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.level").alias("employee_level"),
    )
)

enriched = (
    with_dims
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.*",
        F.col("p.email").alias("employee_email_2"),
        F.col("p.salary").alias("employee_salary"),
        F.col("p.level").alias("employee_level_2"),
    )
)

enriched_employees = (
    enriched
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .select(
        "s.id",
        "s.full_name",
        "s.email",
        "s.dept_id",
        "s.manager_id",
        "s.salary",
        "s.hire_date",
        "s.running_salary",
        "s.email_user",
        "s.employee_hire_date",
        "s.employee_level",
        "s.employee_email_2",
        "s.employee_salary",
        "s.employee_level_2",
        F.col("e.email").alias("employee_email_3"),
    )
)

enriched_employees_2 = (
    enriched_employees
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.level"),
        F.col("d.email").alias("employee_email"),
        F.col("d.full_name").alias("employee_full_name"),
    )
)

deduped = (
    enriched_employees_2
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "email_user",
        "employee_hire_date",
        "employee_level",
        "employee_email_2",
        "employee_salary",
        "employee_email_3",
    )
)

result = (
    deduped
    .alias("s")
    .join(df_departments.alias("dim"), F.col("s.dept_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.dept_name").alias("dept_name"),
        F.col("s.employee_level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("s.salary")).alias("total_salary"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
