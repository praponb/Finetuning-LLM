from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-3, -1)
w_running = Window.partitionBy("employee_job_title").orderBy("salary_avg_2", "id")
w_hist = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-6, -1)
w_hist_2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("job_title")
w7 = Window.orderBy("salary_avg_3_prior", "id")
w8 = Window.partitionBy("employee_job_title").orderBy(F.col("salary_avg_3_prior_bucket").asc_nulls_last(), "id")
w9 = Window.partitionBy("employee_job_title").orderBy(F.col("salary_avg_2_bucket").desc_nulls_first(), "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-06").cast("date")) & F.col("job_title").isNotNull())
    .select("*", F.initcap(F.trim("full_name")).alias("full_name_title"))
)

with_dims = (
    base
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.email").alias("employee_email"),
        F.col("b.job_title").alias("employee_job_title"),
        F.col("b.salary").alias("employee_salary"),
    )
)

with_windows = (
    with_dims
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_2"),
        F.avg("salary").over(w2).alias("salary_avg_3_prior"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.ntile(3).over(w_running).alias("salary_avg_2_bucket"),
        F.avg("salary_avg_2").over(w_hist).alias("salary_avg_2_avg_6_prior"),
        F.sum("salary").over(w_hist_2).alias("running_salary"),
        F.round((F.col("salary_avg_2") / F.sum("salary_avg_2").over(w6)), 4).alias("salary_avg_2_share"),
        F.ntile(10).over(w7).alias("salary_avg_3_prior_bucket"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

ranked_employees = (
    ranked
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked_employees
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.col("max_employee_salary"),
    )
    .orderBy("job_title")
)
