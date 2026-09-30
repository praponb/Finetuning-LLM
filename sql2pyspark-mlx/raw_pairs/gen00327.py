from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("hire_date")).orderBy("employee_salary", "id")
w_running = Window.partitionBy("job_title").orderBy("employee_salary", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy("employee_salary", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("level").orderBy(F.col("last4_sum_employee_salary").asc_nulls_last(), "id")
w5 = Window.partitionBy("location").orderBy("last4_min_employee_salary", "id")
w6 = Window.partitionBy(F.month("hire_date"), "manager_id").orderBy("last4_max_employee_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

with_dims = (
    df_employees
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "left")
    .filter(F.col("d.email").like("%@startup.io"))
    .select(
        "s.*",
        F.col("d.salary").alias("employee_salary"),
        F.col("d.email").alias("employee_email"),
    )
)

with_windows = (
    with_dims
    .withColumn("next_employee_salary", F.lead("employee_salary", 1).over(w))
    .withColumn("salary_avg_2", F.avg("salary").over(w_running))
)

scored = (
    with_windows
    .withColumn("last4_sum_employee_salary", F.sum("employee_salary").over(w3))
    .withColumn("last4_min_employee_salary", F.min("employee_salary").over(w3))
    .withColumn("last4_max_employee_salary", F.max("employee_salary").over(w3))
)

with_dims_employees = (
    scored
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.budget"), F.col("d.location"))
)

ranked = (
    with_dims_employees
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

valid = (
    ranked
    .filter(F.col("hire_date") > F.lit("2023-04-03").cast("date"))
    .withColumn("row_num_rounded", F.round("row_num", 1))
    .withColumn("location_clean", F.coalesce("location", F.lit("OTHER")))
)

scored_employees = (
    valid
    .select(
        "id",
        "dept_id",
        "salary",
        "employee_salary",
        "employee_email",
        "next_employee_salary",
        "last4_sum_employee_salary",
        "last4_min_employee_salary",
        "last4_max_employee_salary",
        "location",
        "row_num_rounded",
        "location_clean",
        F.ntile(5).over(w5).alias("last4_min_employee_salary_bucket"),
        F.first("level", ignorenulls=True).over(w6).alias("first_known_level"),
    )
)

result = (
    scored_employees
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "salary",
        "employee_salary",
        "employee_email",
        "next_employee_salary",
        "last4_sum_employee_salary",
        "last4_min_employee_salary",
        "last4_max_employee_salary",
        F.col("avg_employee_salary"),
    )
    .orderBy(F.col("avg_employee_salary").desc_nulls_first())
)
