from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("salary") <= 92000))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        F.substring("job_title", 1, 3).alias("job_title_prefix"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.email",
        "e.dept_id",
        "e.level",
        "e.job_title_prefix",
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("d.location"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .filter(F.col("b.budget").between(1972000, 3425000))
    .select("e.*", F.col("b.dept_name").alias("department_dept_name"))
)

with_windows = (
    joined
    .select(
        "id",
        "full_name",
        "dept_id",
        "department_dept_name",
        F.min("budget").over(w_part).alias("last7_min_budget"),
        F.count("budget").over(w_part).alias("last7_count_budget"),
        F.sum("budget").over(w_part).alias("last7_sum_budget"),
        F.max("budget").over(w_part).alias("last7_max_budget"),
    )
)

result = (
    with_windows
    .alias("a")
    .join(df_departments.alias("dim"), F.col("a.dept_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["dept_name", "department_dept_name"], ["dept_name"]],
        F.col("dim.dept_name").alias("dept_name"),
        F.col("a.department_dept_name").alias("department_dept_name"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("a.last7_min_budget")), 2).alias("total_last7_min_budget"),
        F.grouping_id().alias("gid"),
    )
)
