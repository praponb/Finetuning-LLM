from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("days_since_hire_date").desc(), "id")
w_part = Window.partitionBy("level", "dept_id").orderBy("cumulative_days_since_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("hire_date")).orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-02-24").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.lower("job_title").alias("job_title_lower"),
    )
)

with_windows = (
    base
    .filter(F.length("full_name") < 14)
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.sum("days_since_hire_date").over(w2).alias("cumulative_days_since_hire_date"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

with_windows_employees = (
    with_windows
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "job_title_lower",
        "seq_no",
        F.avg("cumulative_days_since_hire_date").over(w_part).alias("running_avg_cumulative_days_since_hire_date"),
        F.min("cumulative_days_since_hire_date").over(w_part).alias("running_min_cumulative_days_since_hire_date"),
        F.sum("cumulative_days_since_hire_date").over(w_part).alias("running_sum_cumulative_days_since_hire_date"),
        F.count("cumulative_days_since_hire_date").over(w_part).alias("running_count_cumulative_days_since_hire_date"),
        F.sum("row_seq").over(w5).alias("running_row_seq"),
    )
)

with_dims = (
    with_windows_employees
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.budget"))
)

result = (
    with_dims
    .select(
        "id",
        "full_name",
        "dept_id",
        "running_sum_cumulative_days_since_hire_date",
        "running_count_cumulative_days_since_hire_date",
        "running_row_seq",
        "budget",
        F.when(F.col("full_name").rlike("^(Olivia Garcia|Jack Taylor)$"), "PRIORITY").otherwise("CORE").alias("full_name_zone"),
    )
    .orderBy(F.col("full_name_zone").desc())
)
