from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("employee_job_title", "dept_name").orderBy(F.col("hire_date").desc_nulls_first(), "id")

latest = (
    df_departments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_name")
)

with_latest = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.hire_date"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)

latest_departments = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "dept_name", "employee_job_title")
)

base = (
    latest_departments
    .filter(F.col("employee_job_title").isin(["Analyst", "Designer", "Engineer"]))
    .withColumn("dept_name_lower", F.lower("dept_name"))
)

result = (
    base
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"),
        F.size(F.collect_set("employee_job_title")).alias("num_employee_job_titles"),
    )
    .orderBy(F.col("num_employee_job_titles").desc_nulls_last())
)
