from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc())

base = (
    df_employees
    .filter(F.col("salary") > 60000)
    .select("*", F.upper("full_name").alias("full_name_upper"))
)

deduped = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

joined = (
    deduped
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.dept_name"), F.col("d.location"), F.col("d.budget"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .filter(~F.col("b.location").isin(["London", "Singapore", "Chiang Mai"]))
    .select(
        "s.id",
        "s.full_name",
        "s.email",
        "s.dept_id",
        "s.manager_id",
        "s.job_title",
        "s.full_name_upper",
        "s.dept_name",
        "s.location",
        F.col("b.dept_name").alias("department_dept_name"),
        F.col("b.budget").alias("department_budget"),
        F.col("b.location").alias("department_location"),
    )
)

with_dims_employees = (
    with_dims
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.full_name").alias("employee_full_name"))
)

result = (
    with_dims_employees
    .alias("s")
    .join(df_departments.alias("dim"), F.col("s.dept_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["department_location", "employee_dept_name"], ["department_location"]],
        F.col("dim.location").alias("department_location"),
        F.col("s.dept_name").alias("employee_dept_name"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.department_budget")).alias("min_department_budget"),
    )
    .orderBy(F.col("num_rows").asc_nulls_last())
)
