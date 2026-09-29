from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("dept_id", "job_title").orderBy("adjusted_salary", "id")
w2 = Window.partitionBy("manager_id").orderBy("adjusted_salary", "id")
w3 = Window.partitionBy("job_title").orderBy("adjusted_adjusted_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("prev_adjusted_salary").asc_nulls_last())
w_hist = Window.partitionBy("manager_id").orderBy("adjusted_adjusted_salary", "id")
w6 = Window.partitionBy("level").orderBy(F.col("adjusted_adjusted_salary").desc_nulls_first())
w7 = Window.partitionBy("level", "job_title").orderBy("event_seq", "id")

cleaned = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        F.round((F.col("salary") * 1.1), 2).alias("adjusted_salary"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lag("adjusted_salary", 1).over(w_running).alias("prev_adjusted_salary"),
        F.lead("adjusted_salary", 2).over(w2).alias("next_adjusted_salary"),
    )
)

cleaned_employees = (
    enriched
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "job_title_clean",
        "prev_adjusted_salary",
        F.round((F.col("adjusted_salary") * 0.85), 2).alias("adjusted_adjusted_salary"),
    )
)

with_dims = (
    cleaned_employees
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.manager_id",
        "a.job_title",
        "a.prev_adjusted_salary",
        "a.adjusted_adjusted_salary",
        F.col("d.full_name").alias("employee_full_name"),
        F.col("d.job_title").alias("employee_job_title"),
        F.col("d.level"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "job_title",
        "adjusted_adjusted_salary",
        "level",
        F.sum("prev_adjusted_salary").over(w3).alias("cumulative_prev_adjusted_salary"),
        F.cume_dist().over(w4).alias("prev_adjusted_salary_cume_dist"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

result = (
    metrics
    .filter(F.col("adjusted_adjusted_salary") >= 56)
    .select(
        "id",
        "job_title",
        "level",
        "cumulative_prev_adjusted_salary",
        "event_seq",
        F.cume_dist().over(w6).alias("adjusted_adjusted_salary_cume_dist"),
        F.lead("prev_adjusted_salary_cume_dist", 2, 0).over(w7).alias("next_prev_adjusted_salary_cume_dist"),
    )
)
