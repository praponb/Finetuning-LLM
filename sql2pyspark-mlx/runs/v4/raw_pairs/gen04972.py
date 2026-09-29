from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("days_since_hire_date").asc_nulls_last())
w_grp = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("employee_level").orderBy("hire_date", "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

cleaned = (
    df_employees
    .filter(F.col("hire_date") <= "2024-03-14")
    .withColumn("full_name_prefix", F.substring("full_name", 1, 3))
    .withColumn("days_since_hire_date", F.datediff(F.current_date(), "hire_date"))
)

joined = (
    cleaned
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .select("s.*", F.col("e.level").alias("employee_level"))
)

scored = (
    joined
    .withColumn("days_since_hire_date_dense_rank", F.dense_rank().over(w))
    .withColumn("running_salary", F.sum("salary").over(w_grp))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w_hist))
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

remaining_keys = (
    latest
    .select("id", "job_title")
    .intersectAll(
        df_employees
        .filter(F.col("hire_date") <= "2024-12-31")
        .select("id", "job_title")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    reconciled
    .select("dept_id")
    .intersectAll(
        df_employees
        .filter(F.col("salary") >= 149000)
        .select("dept_id")
    )
)

reconciled_employees = (
    reconciled
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled_employees
    .filter(F.col("level").isNotNull() & (F.col("manager_id") == 17))
    .select(
        "*",
        F.substring("full_name", 1, 4).alias("full_name_prefix_2"),
        F.coalesce("employee_level", F.lit("N/A")).alias("employee_level_filled"),
    )
)

result = (
    filtered
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        "full_name_prefix",
        "days_since_hire_date",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("days_since_hire_date").desc())
)
