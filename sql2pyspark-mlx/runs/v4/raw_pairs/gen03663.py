from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy("salary", "id")
w3 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id").orderBy("running_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level")
w6 = Window.partitionBy("job_title").orderBy(F.col("running_salary").desc(), "id")
w7 = Window.partitionBy("dept_id").orderBy("running_salary")
w8 = Window.orderBy("event_seq", "id")
w9 = Window.partitionBy("job_title")
w_part = Window.partitionBy("manager_id").orderBy("prev_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("job_title").orderBy(F.col("cumulative_prev_salary").asc_nulls_last())
w12 = Window.partitionBy("manager_id").orderBy("first_known_cumulative_prev_salary", "id").rowsBetween(-5, Window.currentRow)
w13 = Window.partitionBy("manager_id").orderBy("cumulative_prev_salary", "id")
w14 = Window.partitionBy("level", "manager_id").orderBy(F.col("cumulative_prev_salary").asc_nulls_first())
w15 = Window.partitionBy("job_title").orderBy("prev_salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

metrics = (
    df_employees
    .withColumn("running_salary", F.sum("salary").over(w))
    .withColumn("prev_salary", F.lag("salary", 1).over(w2))
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w3))
)

with_windows = (
    metrics
    .withColumn("cumulative_prev_salary", F.sum("prev_salary").over(w4))
    .withColumn("prev_salary_share", F.round((F.col("prev_salary") / F.sum("prev_salary").over(w5)), 4))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("running_salary_rank", F.rank().over(w7))
)

with_windows_employees = (
    with_windows
    .select(
        "*",
        F.lag("prev_salary", 1).over(w8).alias("prev_prev_salary"),
        F.min("running_salary_rank").over(w9).alias("job_title_min_running_salary_rank"),
        F.first("cumulative_prev_salary", ignorenulls=True).over(w_part).alias("first_known_cumulative_prev_salary"),
        F.dense_rank().over(w_hist).alias("cumulative_prev_salary_dense_rank"),
    )
)

enriched = (
    with_windows_employees
    .select(
        "*",
        F.avg("running_salary").over(w9).alias("job_title_avg_running_salary"),
        F.avg("cumulative_prev_salary").over(w12).alias("cumulative_prev_salary_avg_5"),
        F.row_number().over(w13).alias("row_seq"),
        F.dense_rank().over(w14).alias("cumulative_prev_salary_dense_rank_2"),
        F.last("level", ignorenulls=True).over(w15).alias("last_known_level"),
    )
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "running_salary",
        F.when(F.col("level").rlike("^(L4|L3)$"), "CORE").when(F.col("level").rlike("^(L1|L2)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("level_zone"),
        F.col("max_employee_salary"),
    )
)
