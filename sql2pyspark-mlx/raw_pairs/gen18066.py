from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id")

base = (
    df_employees
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-08-28")))
    .select("*", F.col("is_remote").cast("int").alias("is_remote_flag"))
)

base_employees = (
    base
    .filter(F.col("job_title").isNotNull())
    .withColumn("job_title_full_name_key", F.concat_ws(",", "job_title", "full_name"))
    .withColumn("job_title_title", F.initcap(F.trim("job_title")))
)

joined = (
    base_employees
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name") == "Finance")
    .select(
        "src.id",
        "src.full_name",
        "src.dept_id",
        "src.manager_id",
        "src.hire_date",
        "src.is_remote",
        "src.is_remote_flag",
        "src.job_title_full_name_key",
        F.col("d.location"),
        F.col("d.budget"),
        F.col("d.dept_name"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        "is_remote_flag",
        "budget",
        "dept_name",
        F.sum("budget").over(w).alias("cumulative_budget"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
)

joined_employees = (
    with_windows
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.full_name").alias("employee_full_name"))
)

result = (
    joined_employees
    .rollup(
        F.when(F.col("cumulative_budget") >= 20638875, "HIGH").otherwise("LOW").alias("cumulative_budget_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("is_remote_flag"), 2).alias("avg_is_remote_flag"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Marketing", 1)), F.lit(0)), 2).alias("budget_per_marketing"),
    )
    .filter(F.col("num_dept_names") > 1)
    .orderBy("cnt")
)
