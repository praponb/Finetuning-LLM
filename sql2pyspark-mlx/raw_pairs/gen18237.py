from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_remote_flag", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy(F.col("employee_salary").desc_nulls_first())
w_ordered = Window.orderBy("last7_min_employee_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("hire_date")).orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-08-31").cast("date")) & (F.col("job_title") == "Analyst"))
    .select(
        "*",
        F.round("salary", 0).alias("salary_rounded"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_clean"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.email",
        "a.manager_id",
        "a.hire_date",
        "a.salary_rounded",
        "a.is_remote_flag",
        "a.job_title_clean",
        F.col("p.salary").alias("employee_salary"),
    )
)

enriched = (
    joined
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "inner")
    .select("e.*", F.col("p2.full_name").alias("employee_full_name"))
)

filtered = (
    enriched
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "email",
        "hire_date",
        "is_remote_flag",
        "job_title_clean",
        "employee_salary",
        "employee_full_name",
        F.initcap(F.trim("full_name")).alias("full_name_title"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.count("employee_salary").over(w).alias("last7_count_employee_salary"),
        F.max("employee_salary").over(w).alias("last7_max_employee_salary"),
        F.min("employee_salary").over(w).alias("last7_min_employee_salary"),
        F.cume_dist().over(w2).alias("employee_salary_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "is_remote_flag",
        "job_title_clean",
        "employee_salary",
        "employee_full_name",
        F.avg("employee_salary_cume_dist").over(w_ordered).alias("running_avg_employee_salary_cume_dist"),
        F.min("employee_salary_cume_dist").over(w_ordered).alias("running_min_employee_salary_cume_dist"),
        F.count("employee_salary_cume_dist").over(w_ordered).alias("running_count_employee_salary_cume_dist"),
        F.last("last7_min_employee_salary", ignorenulls=True).over(w4).alias("last_known_last7_min_employee_salary"),
    )
)
