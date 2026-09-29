from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("email_length").desc_nulls_first())
w2 = Window.partitionBy("dept_id").orderBy("email_length", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

filtered = (
    df_employees
    .filter(F.col("salary") > 111000)
    .withColumn("email_length", F.length("email"))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("NONE")))
)

metrics = (
    filtered
    .filter(F.col("salary") < 59000)
    .select(
        "*",
        F.dense_rank().over(w).alias("email_length_dense_rank"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.location"), F.col("d.dept_name"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.dept_id",
        "a.job_title",
        "a.hire_date",
        "a.is_remote",
        "a.email_length",
        "a.resolved_job_title",
        "a.email_length_dense_rank",
        "a.location",
        "a.dept_name",
        F.col("d.dept_name").alias("department_dept_name"),
        F.col("d.location").alias("department_location"),
    )
)

joined_employees = (
    joined
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.dept_id",
        "src.is_remote",
        "src.email_length",
        "src.resolved_job_title",
        "src.location",
        "src.department_dept_name",
        F.col("d.location").alias("department_location_2"),
        F.col("d.budget"),
        F.col("d.dept_name").alias("department_dept_name_2"),
    )
)

result = (
    joined_employees
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "email_length",
        "resolved_job_title",
        "location",
        "department_dept_name",
        "budget",
        F.col("min_employee_salary"),
    )
)
