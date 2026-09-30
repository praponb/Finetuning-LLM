from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w_part = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("YEAR", "hire_date_plus_months"), "dept_id").orderBy("salary", "id")
w4 = Window.partitionBy("job_title", "manager_id").orderBy(F.col("hire_date_plus_months").desc(), "id")
w_grp = Window.partitionBy("manager_id").orderBy("last4_avg_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("hire_date_plus_months"), "job_title").orderBy(F.col("last4_count_salary").desc(), "id")
w7 = Window.partitionBy("dept_id")
w_recent = Window.partitionBy("manager_id")

valid = (
    df_employees
    .filter(F.col("email").like("%@yahoo.com"))
    .withColumn("hire_date_plus_months", F.add_months("hire_date", 3))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("DIRECT")))
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "hire_date_plus_months",
        F.max("salary").over(w_part).alias("last4_max_salary"),
        F.avg("salary").over(w_part).alias("last4_avg_salary"),
        F.count("salary").over(w_part).alias("last4_count_salary"),
        F.ntile(5).over(w3).alias("salary_bucket"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "is_remote",
        "last4_max_salary",
        "last4_avg_salary",
        "salary_bucket",
        F.row_number().over(w4).alias("event_seq"),
        F.first("last4_max_salary", ignorenulls=True).over(w_grp).alias("first_known_last4_max_salary"),
        F.ntile(3).over(w6).alias("last4_count_salary_bucket"),
        F.round((F.col("salary") / F.sum("salary").over(w7)), 4).alias("salary_share"),
        F.max("last4_max_salary").over(w_recent).alias("manager_id_max_last4_max_salary"),
    )
)

result = (
    scored
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.location").alias("location"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.event_seq")).alias("max_event_seq"),
    )
    .orderBy(F.col("location").desc_nulls_last())
)
