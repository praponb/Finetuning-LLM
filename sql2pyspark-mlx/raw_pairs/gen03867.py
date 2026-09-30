from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("location", "dept_name").orderBy("is_remote_flag", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("level", "dept_name").orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location", "job_title").orderBy(F.col("last3_count_salary").desc(), "id")
w_running = Window.partitionBy("location", "job_title").orderBy(F.col("budget_scaled").asc_nulls_last())
w6 = Window.partitionBy("job_title").orderBy(F.col("last3_max_salary").desc(), "id")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-07-23").cast("date")) & F.col("dept_id").isin([28, 37]))
    .select(
        "*",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location").isNotNull())
    .select(
        "src.id",
        "src.dept_id",
        "src.job_title",
        "src.salary",
        "src.hire_date",
        "src.is_remote",
        "src.level",
        "src.is_remote_flag",
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
)

scored = (
    joined
    .select(
        "id",
        "job_title",
        "is_remote",
        "budget",
        "location",
        "dept_name",
        F.sum("salary").over(w_part).alias("last3_sum_salary"),
        F.count("salary").over(w_part).alias("last3_count_salary"),
        F.max("salary").over(w_part).alias("last3_max_salary"),
        F.avg("salary").over(w_part).alias("last3_avg_salary"),
        F.last("job_title", ignorenulls=True).over(w_grp).alias("last_known_job_title"),
    )
)

cleaned = (
    scored
    .filter(F.col("is_remote"))
    .select(
        "id",
        "job_title",
        "is_remote",
        "location",
        "last3_sum_salary",
        "last3_count_salary",
        "last3_max_salary",
        "last_known_job_title",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.round((F.col("budget") * 0.85), 2).alias("budget_scaled"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

base = (
    cleaned
    .filter(F.col("job_title").isNotNull())
    .withColumn("is_remote_flag_times_last3_count_salary", (F.col("is_remote_flag") * F.col("last3_count_salary")))
    .withColumn("resolved_location", F.coalesce("location", F.lit("N/A")))
)

result = (
    base
    .select(
        "id",
        "job_title",
        "location",
        "last3_sum_salary",
        "last3_count_salary",
        "last3_max_salary",
        "budget_scaled",
        "resolved_job_title",
        "is_remote_flag_times_last3_count_salary",
        F.sum("last3_max_salary").over(w3).alias("cumulative_last3_max_salary"),
        F.ntile(3).over(w4).alias("last3_count_salary_bucket"),
        F.rank().over(w_running).alias("budget_scaled_rank"),
        F.ntile(10).over(w6).alias("last3_max_salary_bucket"),
    )
)
