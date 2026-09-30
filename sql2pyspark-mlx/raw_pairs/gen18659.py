from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name").orderBy("hire_date", "id")
w3 = Window.partitionBy("dept_name").orderBy(F.col("dept_name_length_per_employee_salary").desc(), "id")

valid = (
    df_departments
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Finance")))
    .select("*", F.length("dept_name").alias("dept_name_length"))
)

filtered = (
    valid
    .filter(F.col("location").isin(["London", "New York", "Berlin"]))
    .withColumn("location_dept_name", F.concat("location", F.lit(" - "), "dept_name"))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.salary").alias("employee_salary"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)

filtered_departments = (
    with_latest
    .filter(F.col("budget") >= 1881000)
    .withColumn("least_dept_name_length_employee_salary", F.least("dept_name_length", "employee_salary"))
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
)

base = (
    filtered_departments
    .filter(F.col("least_dept_name_length_employee_salary") >= 74)
    .withColumn("location_upper", F.upper("location"))
    .withColumn("dept_name_length_per_employee_salary", F.round((F.col("dept_name_length") / F.col("employee_salary")), 2))
)

latest_match = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

metrics = (
    latest_match
    .filter(F.col("budget") <= 1613000)
    .select(
        "id",
        "budget",
        "dept_name_length",
        "location_clean",
        "employee_salary",
        "employee_job_title",
        "least_dept_name_length_employee_salary",
        F.lag("least_dept_name_length_employee_salary", 1).over(w2).alias("prev_least_dept_name_length_employee_salary"),
        F.ntile(10).over(w3).alias("dept_name_length_per_employee_salary_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "budget",
        "location_clean",
        "employee_salary",
        "least_dept_name_length_employee_salary",
        "dept_name_length_per_employee_salary_bucket",
    )
    .orderBy(
        F.col("employee_salary").desc(),
        F.col("least_dept_name_length_employee_salary").desc(),
    )
)
