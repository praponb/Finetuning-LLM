from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_upper", F.upper("dept_name"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("DIRECT")))
)

eligible = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary") > 62000), "left_anti")
)

deduped = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.salary").alias("employee_salary"),
    )
)

eligible_departments = (
    with_latest
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") >= F.col("s.employee_hire_date")), "left_anti")
)

result = (
    eligible_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "dept_name_upper",
        "dept_name_clean",
        "dedup_rank",
        "employee_hire_date",
    )
)
