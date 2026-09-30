from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w_running = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w_recent = Window.partitionBy("dept_id", "manager_id")
w4 = Window.partitionBy("job_title").orderBy("dept_id_avg_salary", "id")
w_part = Window.orderBy("salary_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("manager_id").orderBy("salary_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("salary").desc(), "id")
w8 = Window.partitionBy("job_title").orderBy("manager_id_count_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("manager_id_count_salary").desc(), "id")
w10 = Window.partitionBy("job_title").orderBy("prev_dept_id_avg_salary", "id")

base = (
    df_employees
    .filter(F.col("salary") < 99000)
    .select("*", F.initcap(F.trim("level")).alias("level_title"))
)

metrics = (
    base
    .withColumn("manager_id_count_salary", F.count("salary").over(w))
    .withColumn("salary_percent_rank", F.percent_rank().over(w_running))
    .withColumn("dept_id_avg_salary", F.avg("salary").over(w_recent))
)

metrics_employees = (
    metrics
    .select(
        "*",
        F.lag("dept_id_avg_salary", 1).over(w4).alias("prev_dept_id_avg_salary"),
        F.last("manager_id_count_salary", ignorenulls=True).over(w_part).alias("last_known_manager_id_count_salary"),
    )
)

with_windows = (
    metrics_employees
    .select(
        "*",
        F.last("prev_dept_id_avg_salary", ignorenulls=True).over(w6).alias("last_known_prev_dept_id_avg_salary"),
        F.ntile(4).over(w7).alias("salary_bucket"),
        F.sum("salary_percent_rank").over(w8).alias("cumulative_salary_percent_rank"),
        F.row_number().over(w9).alias("event_seq"),
        F.ntile(10).over(w10).alias("prev_dept_id_avg_salary_bucket"),
    )
)

result = (
    with_windows
    .withColumn("cumulative_salary_percent_rank_tier", F.when(F.col("cumulative_salary_percent_rank") >= 3, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "hire_date"))
    .groupingSets(
        [["cumulative_salary_percent_rank_tier", "dept_id", "activity_month"], ["cumulative_salary_percent_rank_tier", "dept_id"], ["cumulative_salary_percent_rank_tier"], []],
        "cumulative_salary_percent_rank_tier",
        "dept_id",
        "activity_month",
    )
    .agg(
        F.sum(F.when(F.col("level") == "L3", F.col("event_seq")).otherwise(0)).alias("l3_event_seq"),
        F.round(F.sum("dept_id_avg_salary") / F.nullif(F.count(F.when(F.col("job_title") == "Director", 1)), F.lit(0)), 2).alias("dept_id_avg_salary_per_director"),
        F.grouping_id().alias("grouping_id"),
    )
)
