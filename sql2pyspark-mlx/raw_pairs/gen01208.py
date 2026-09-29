from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

base = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-05-05")))
    .withColumn("job_title_upper", F.upper("job_title"))
)

metrics = (
    base
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "job_title_upper",
        F.avg("salary").over(w).alias("last4_avg_salary"),
        F.max("salary").over(w).alias("last4_max_salary"),
        F.count("salary").over(w).alias("last4_count_salary"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") > 2170000)
    .select("src.*", F.col("d.location"), F.col("d.budget"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.dept_name"),
        F.col("b.budget").alias("department_budget"),
        F.col("b.location").alias("department_location"),
    )
)

result = (
    joined
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "job_title_upper",
        "last4_avg_salary",
        "location",
        F.when(F.col("full_name").rlike("^(Henry Ford|Alice Wong)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("full_name_zone"),
        F.col("min_employee_salary"),
    )
)
