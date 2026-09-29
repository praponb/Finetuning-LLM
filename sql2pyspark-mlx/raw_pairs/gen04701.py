from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_level").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("employee_hire_date"), "employee_level")
w4 = Window.partitionBy("employee_level", "dept_name").orderBy("employee_hire_date_month_count_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name")
w6 = Window.partitionBy("dept_name").orderBy(F.col("employee_hire_date_month_count_budget_share").asc_nulls_first())
w7 = Window.partitionBy("dept_name", "employee_level")
w8 = Window.partitionBy("employee_level").orderBy("employee_hire_date_month_count_budget_share", "id")
w9 = Window.partitionBy(F.year("employee_hire_date")).orderBy("running_sum_employee_hire_date_month_count_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("employee_level").orderBy(F.col("employee_hire_date_month_count_budget").desc(), "id")
w11 = Window.partitionBy("dept_name").orderBy(F.col("running_max_employee_hire_date_month_count_budget").desc(), "id")
w12 = Window.partitionBy("employee_level").orderBy("running_sum_employee_hire_date_month_count_budget", "id").rowsBetween(-2, -1)

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.full_name").like("E%")), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.level").alias("employee_level"),
    )
)

metrics = (
    latest_match
    .filter(F.col("location").isin(["Tokyo", "Berlin", "Chiang Mai"]))
    .select(
        "id",
        "dept_name",
        "employee_hire_date",
        "employee_level",
        F.first("location", ignorenulls=True).over(w2).alias("first_known_location"),
        F.count("budget").over(w3).alias("employee_hire_date_month_count_budget"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.count("employee_hire_date_month_count_budget").over(w4).alias("running_count_employee_hire_date_month_count_budget"),
        F.max("employee_hire_date_month_count_budget").over(w4).alias("running_max_employee_hire_date_month_count_budget"),
        F.sum("employee_hire_date_month_count_budget").over(w4).alias("running_sum_employee_hire_date_month_count_budget"),
        F.round((F.col("employee_hire_date_month_count_budget") / F.sum("employee_hire_date_month_count_budget").over(w5)), 4).alias("employee_hire_date_month_count_budget_share"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .withColumn("running_sum_employee_hire_date_month_count_budget_share", F.round((F.col("running_sum_employee_hire_date_month_count_budget") / F.sum("running_sum_employee_hire_date_month_count_budget").over(w7)), 4))
    .withColumn("dept_name_max_running_sum_employee_hire_date_month_count_budget", F.max("running_sum_employee_hire_date_month_count_budget").over(w7))
    .withColumn("employee_hire_date_month_count_budget_share_bucket", F.ntile(3).over(w8))
    .withColumn("last_known_dept_name", F.last("dept_name", ignorenulls=True).over(w9))
    .withColumn("employee_hire_date_month_count_budget_bucket", F.ntile(4).over(w_ordered))
)

scored_departments = (
    with_windows
    .select(
        "id",
        "dept_name",
        "employee_hire_date",
        "employee_level",
        "running_count_employee_hire_date_month_count_budget",
        "running_max_employee_hire_date_month_count_budget",
        "rank_in_group",
        "running_sum_employee_hire_date_month_count_budget_share",
        "dept_name_max_running_sum_employee_hire_date_month_count_budget",
        "employee_hire_date_month_count_budget_share_bucket",
        "employee_hire_date_month_count_budget_bucket",
        F.ntile(4).over(w11).alias("running_max_employee_hire_date_month_count_budget_bucket"),
        F.avg("employee_hire_date_month_count_budget_share_bucket").over(w12).alias("employee_hire_date_month_count_budget_share_bucket_avg_2_prior"),
        F.min("employee_hire_date_month_count_budget_share_bucket").over(w7).alias("dept_name_min_employee_hire_date_month_count_budget_share_bucket"),
    )
)

result = (
    scored_departments
    .select(
        "id",
        "employee_hire_date",
        "running_count_employee_hire_date_month_count_budget",
        "running_max_employee_hire_date_month_count_budget",
        "running_sum_employee_hire_date_month_count_budget_share",
        "dept_name_max_running_sum_employee_hire_date_month_count_budget",
        "employee_hire_date_month_count_budget_share_bucket",
        "employee_hire_date_month_count_budget_bucket",
        "running_max_employee_hire_date_month_count_budget_bucket",
    )
    .orderBy(
        F.col("employee_hire_date_month_count_budget_share_bucket").asc_nulls_last(),
        F.col("id").desc(),
    )
)
