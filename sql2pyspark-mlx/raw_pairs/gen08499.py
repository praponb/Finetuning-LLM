from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level").orderBy("employee_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id", "location")
w3 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w_ordered = Window.partitionBy("location").orderBy("employee_salary", "id")
w6 = Window.partitionBy("dept_name", "manager_id").orderBy(F.col("cumulative_salary").desc_nulls_last())
w7 = Window.partitionBy("job_title")
w8 = Window.orderBy(F.col("cumulative_salary").desc())
w9 = Window.partitionBy("department_dept_name")
w10 = Window.partitionBy("dept_id").orderBy(F.col("salary_share").desc_nulls_first())
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

with_dims = (
    df_employees
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .filter(F.col("p.budget") <= 1372000)
    .select("a.*", F.col("p.location"), F.col("p.dept_name"))
)

with_dims_employees = (
    with_dims
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .select(
        "src.*",
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.email").alias("employee_email"),
    )
)

scored = (
    with_dims_employees
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        "location",
        "dept_name",
        "employee_full_name",
        F.sum("salary").over(w_recent).alias("cumulative_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.sum("salary").over(w3).alias("cumulative_salary_2"),
        F.row_number().over(w4).alias("row_seq"),
        F.ntile(10).over(w_ordered).alias("employee_salary_bucket"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "dept_id",
        "job_title",
        "level",
        "location",
        "dept_name",
        "employee_full_name",
        "cumulative_salary",
        "salary_share",
        "row_seq",
    )
)

matched_keys = (
    deduped
    .select("dept_id", "id")
    .subtract(
        df_employees
        .filter(F.col("hire_date") >= "2024-03-01")
        .select("dept_id", "id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.dept_name").alias("department_dept_name"))
)

with_windows = (
    enriched
    .withColumn("job_title_avg_salary_share", F.avg("salary_share").over(w7))
    .withColumn("cumulative_salary_percent_rank", F.percent_rank().over(w8))
    .withColumn("department_dept_name_min_salary_share", F.min("salary_share").over(w9))
    .withColumn("salary_share_cume_dist", F.cume_dist().over(w10))
)

result = (
    with_windows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "job_title",
        "level",
        "location",
        "dept_name",
        "employee_full_name",
        "row_seq",
        "job_title_avg_salary_share",
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("row_seq").desc_nulls_last())
)
