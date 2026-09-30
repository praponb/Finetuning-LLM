from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("manager_id", "job_title").orderBy("job_title_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy("salary", "id")
w_hist = Window.orderBy("running_avg_job_title_length", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("dept_id").orderBy(F.col("running_max_job_title_length").desc_nulls_last())
w7 = Window.partitionBy("dept_id")
w8 = Window.partitionBy("dept_id").orderBy("running_count_job_title_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("dept_id").orderBy("running_min_job_title_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & F.col("manager_id").isin([7, 9, 20, 39]))
    .select(
        "*",
        F.length("job_title").alias("job_title_length"),
        F.coalesce("level", F.lit("DIRECT")).alias("resolved_level"),
    )
)

metrics = (
    valid
    .filter(F.col("salary").between(117000, 140000))
    .withColumn("running_avg_job_title_length", F.avg("job_title_length").over(w_running))
    .withColumn("running_min_job_title_length", F.min("job_title_length").over(w_running))
    .withColumn("running_max_job_title_length", F.max("job_title_length").over(w_running))
    .withColumn("running_count_job_title_length", F.count("job_title_length").over(w_running))
    .withColumn("first_known_level", F.first("level", ignorenulls=True).over(w_ordered))
)

metrics_employees = (
    metrics
    .select(
        "id",
        "email",
        "dept_id",
        "hire_date",
        "running_min_job_title_length",
        "running_max_job_title_length",
        "running_count_job_title_length",
        F.last("running_count_job_title_length", ignorenulls=True).over(w_part).alias("last_known_running_count_job_title_length"),
        F.lag("job_title_length", 2).over(w4).alias("prev_job_title_length"),
        F.avg("salary").over(w_hist).alias("salary_avg_5_prior"),
        F.cume_dist().over(w6).alias("running_max_job_title_length_cume_dist"),
    )
)

metrics_employees_2 = (
    metrics_employees
    .select(
        "id",
        "email",
        "dept_id",
        "hire_date",
        "running_min_job_title_length",
        "running_count_job_title_length",
        "last_known_running_count_job_title_length",
        "prev_job_title_length",
        F.min("prev_job_title_length").over(w7).alias("dept_id_min_prev_job_title_length"),
        F.sum("last_known_running_count_job_title_length").over(w8).alias("running_last_known_running_count_job_title_length"),
    )
)

scored = (
    metrics_employees_2
    .select(
        "*",
        F.min("prev_job_title_length").over(w8).alias("running_min_prev_job_title_length"),
        F.sum("prev_job_title_length").over(w8).alias("running_sum_prev_job_title_length"),
        F.max("prev_job_title_length").over(w8).alias("running_max_prev_job_title_length"),
        F.last("running_last_known_running_count_job_title_length", ignorenulls=True).over(w_recent).alias("last_known_running_last_known_running_count_job_title_length"),
    )
)

base = (
    scored
    .filter((F.col("hire_date") >= F.lit("2023-05-16").cast("date")) & F.col("running_max_prev_job_title_length").isNotNull())
    .select(
        "*",
        F.ceil("running_min_prev_job_title_length").alias("running_min_prev_job_title_length_whole"),
    )
)

result = (
    base
    .alias("src")
    .join(df_departments.alias("dim"), F.col("src.dept_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["dept_name", "location"], ["dept_name"], []],
        F.col("dim.dept_name").alias("dept_name"),
        F.col("dim.location").alias("location"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.last_known_running_count_job_title_length")).alias("min_last_known_running_count_job_title_length"),
        F.max(F.col("src.dept_id_min_prev_job_title_length")).alias("max_dept_id_min_prev_job_title_length"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("location")
)
