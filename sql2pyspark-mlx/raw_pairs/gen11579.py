from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("hire_date"), "manager_id").orderBy(F.col("days_since_hire_date").asc_nulls_last())
w2 = Window.partitionBy("level").orderBy(F.col("days_since_hire_date").desc(), "id")
w3 = Window.partitionBy("level").orderBy(F.col("days_since_hire_date").asc_nulls_first())
w4 = Window.partitionBy("level").orderBy("days_since_hire_date_dense_rank", "id")
w5 = Window.orderBy("event_seq")

base = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-08-26").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
    )
)

with_windows = (
    base
    .filter(F.col("manager_id").isin([4, 13, 30]))
    .withColumn("days_since_hire_date_cume_dist", F.cume_dist().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("days_since_hire_date_dense_rank", F.dense_rank().over(w3))
)

joined = (
    with_windows
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.budget"))
)

enriched = (
    joined
    .select(
        "id",
        "dept_id",
        "manager_id",
        "hire_date",
        "days_since_hire_date",
        "days_since_hire_date_dense_rank",
        F.lag("days_since_hire_date_cume_dist").over(w4).alias("prev_days_since_hire_date_cume_dist"),
        F.lag("event_seq").over(w4).alias("prev_event_seq"),
        F.lead("event_seq").over(w4).alias("next_event_seq"),
        F.row_number().over(w4).alias("seq_no"),
        F.dense_rank().over(w5).alias("event_seq_dense_rank"),
    )
)

joined_employees = (
    enriched
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "src.id",
        "src.dept_id",
        "src.manager_id",
        "src.hire_date",
        "src.days_since_hire_date_dense_rank",
        "src.prev_days_since_hire_date_cume_dist",
        "src.prev_event_seq",
        "src.seq_no",
        "src.event_seq_dense_rank",
        F.col("d.email"),
        F.col("d.level"),
    )
)

result = (
    joined_employees
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["job_title", "employee_level"], ["job_title"]],
        F.col("p.job_title").alias("job_title"),
        F.col("e.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("e.prev_days_since_hire_date_cume_dist")), 2).alias("avg_prev_days_since_hire_date_cume_dist"),
        F.grouping_id().alias("gid"),
    )
)
