from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy(F.col("running_max_salary").asc_nulls_first())
w4 = Window.orderBy("running_count_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("MONTH", "hire_date"), "dept_id").orderBy(F.col("running_count_salary").desc(), "id")
w_hist = Window.partitionBy("dept_id").orderBy("running_max_salary", "id")
w7 = Window.partitionBy("dept_id").orderBy("running_max_salary_bucket", "id")
w8 = Window.orderBy(F.col("row_seq").desc(), "id")
w9 = Window.orderBy(F.col("running_max_salary").desc(), "id")
w10 = Window.partitionBy("dept_id")
w11 = Window.partitionBy("dept_id").orderBy("running_max_salary_share", "id").rowsBetween(-3, Window.currentRow)
w_part = Window.partitionBy("dept_id").orderBy("seq_no_2", "id")
w13 = Window.partitionBy("dept_id").orderBy("running_max_salary_share", "id")
w14 = Window.partitionBy("dept_id").orderBy(F.col("row_seq_2_avg_3").desc(), "id")
w15 = Window.orderBy("prev_running_max_salary", "id").rowsBetween(-3, -1)
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

scored = (
    df_employees
    .filter(F.col("email").like("%@mail.co.th"))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.avg("salary").over(w).alias("running_avg_salary"),
        F.max("salary").over(w).alias("running_max_salary"),
        F.count("salary").over(w).alias("running_count_salary"),
        F.first("level", ignorenulls=True).over(w2).alias("first_known_level"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "full_name",
        "dept_id",
        "hire_date",
        "running_max_salary",
        F.cume_dist().over(w_ordered).alias("running_max_salary_cume_dist"),
        F.last("running_count_salary", ignorenulls=True).over(w4).alias("last_known_running_count_salary"),
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(4).over(w_hist).alias("running_max_salary_bucket"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "dept_id",
        "running_max_salary",
        "row_seq",
        F.row_number().over(w7).alias("seq_no"),
        F.row_number().over(w8).alias("seq_no_2"),
        F.row_number().over(w9).alias("row_seq_2"),
        F.round((F.col("running_max_salary") / F.sum("running_max_salary").over(w10)), 4).alias("running_max_salary_share"),
    )
)

metrics_employees = (
    metrics
    .select(
        "id",
        "dept_id",
        "running_max_salary",
        "seq_no_2",
        "running_max_salary_share",
        F.avg("row_seq_2").over(w11).alias("row_seq_2_avg_3"),
        F.ntile(10).over(w_part).alias("seq_no_2_bucket"),
        F.lag("running_max_salary", 2).over(w13).alias("prev_running_max_salary"),
    )
)

scored_employees = (
    metrics_employees
    .select(
        "id",
        "dept_id",
        "running_max_salary",
        "row_seq_2_avg_3",
        F.ntile(5).over(w_hist).alias("running_max_salary_bucket"),
        F.ntile(4).over(w14).alias("row_seq_2_avg_3_bucket"),
        F.avg("prev_running_max_salary").over(w15).alias("prev_running_max_salary_avg_3_prior"),
    )
)

result = (
    scored_employees
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "running_max_salary",
        "row_seq_2_avg_3",
        "running_max_salary_bucket",
        "row_seq_2_avg_3_bucket",
        "prev_running_max_salary_avg_3_prior",
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("row_seq_2_avg_3").asc_nulls_last())
)
