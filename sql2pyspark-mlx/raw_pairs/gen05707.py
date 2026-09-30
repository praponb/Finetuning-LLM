from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("hire_date"), "dept_id").orderBy("salary", "id")
w2 = Window.orderBy("hire_date", "id").rowsBetween(-5, -1)
w_running = Window.partitionBy("dept_id").orderBy("salary", "id")
w4 = Window.partitionBy(F.month("hire_date")).orderBy("salary", "id")
w5 = Window.orderBy("salary_avg_5_prior", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.orderBy("prev_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("prev_salary_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.month("hire_date"), "manager_id")
w9 = Window.partitionBy("level")

scored = (
    df_employees
    .select(
        "*",
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.avg("salary").over(w2).alias("salary_avg_5_prior"),
        F.lag("salary", 1, 0).over(w_running).alias("prev_salary_2"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        "prev_salary",
        "prev_salary_2",
        F.lead("prev_salary", 2, 0).over(w4).alias("next_prev_salary"),
        F.avg("salary_avg_5_prior").over(w5).alias("salary_avg_5_prior_avg_5"),
        F.sum("salary_avg_5_prior").over(w6).alias("cumulative_salary_avg_5_prior"),
        F.last("prev_salary", ignorenulls=True).over(w7).alias("last_known_prev_salary"),
        F.round((F.col("prev_salary_2") / F.sum("prev_salary_2").over(w8)), 4).alias("prev_salary_2_share"),
    )
)

kept_keys = (
    with_windows
    .select("job_title")
    .intersect(
        df_employees
        .filter(F.upper("full_name").like("%OB %"))
        .select("job_title")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "id",
        "manager_id",
        "job_title",
        "level",
        "prev_salary",
        "prev_salary_2",
        "next_prev_salary",
        "salary_avg_5_prior_avg_5",
        "last_known_prev_salary",
        "prev_salary_2_share",
        F.avg("prev_salary_2").over(w9).alias("level_avg_prev_salary_2"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .filter(F.col("e.email").like("%@yahoo.com"))
    .select(
        "a.*",
        F.col("e.salary"),
        F.col("e.level").alias("employee_level"),
        F.col("e.full_name"),
    )
)

result = (
    with_dims
    .groupBy("job_title")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
    )
)
