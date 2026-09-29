from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id")
w5 = Window.partitionBy("manager_id", "job_title").orderBy("manager_id_max_salary", "id")
w6 = Window.partitionBy("manager_id", "job_title").orderBy("cumulative_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("hire_date").desc(), "id")
w8 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-6, -1)
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-03-04").cast("date")) & F.col("level").isNotNull())
    .withColumn("clean_level", F.trim("level"))
)

enriched = (
    valid
    .select(
        "*",
        F.avg("salary").over(w_hist).alias("salary_avg_2"),
        F.last("job_title", ignorenulls=True).over(w_ordered).alias("last_known_job_title"),
        F.sum("salary").over(w_grp).alias("cumulative_salary"),
        F.max("salary").over(w4).alias("manager_id_max_salary"),
    )
)

with_windows = (
    enriched
    .withColumn("manager_id_max_salary_bucket", F.ntile(5).over(w5))
    .withColumn("running_salary", F.sum("salary").over(w6))
    .withColumn("event_seq", F.row_number().over(w7))
    .withColumn("cumulative_salary_avg_6_prior", F.avg("cumulative_salary").over(w8))
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.budget"), F.col("d.location"), F.col("d.dept_name"))
)

valid_employees = (
    with_dims
    .filter((F.col("hire_date") >= F.to_date(F.lit("2024-03-01"))) & F.col("location").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "clean_level",
        "last_known_job_title",
        "cumulative_salary",
        "manager_id_max_salary",
        "running_salary",
        "cumulative_salary_avg_6_prior",
        "budget",
        "location",
        "dept_name",
        F.length("location").alias("location_length"),
        F.coalesce("event_seq", F.lit(0)).alias("event_seq_filled"),
    )
)

result = (
    valid_employees
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "hire_date",
        "is_remote",
        "clean_level",
        "last_known_job_title",
        "cumulative_salary",
        F.col("sum_employee_salary"),
    )
)
