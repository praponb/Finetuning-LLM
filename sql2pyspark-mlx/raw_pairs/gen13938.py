from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("dept_name").orderBy(F.col("employee_salary").desc_nulls_first())
w4 = Window.partitionBy("employee_level").orderBy("budget", "id")
w5 = Window.partitionBy("employee_level").orderBy("employee_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("employee_level").orderBy(F.col("cumulative_budget_whole").asc_nulls_first(), "id")

valid = (
    df_departments
    .filter(F.col("budget") < 3382000)
    .select(
        "*",
        F.length("dept_name").alias("dept_name_length"),
        F.floor("budget").alias("budget_whole"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget",
        "s.budget_whole",
        F.col("c.level").alias("employee_level"),
    )
)

latest_match_departments = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"))
)

with_windows = (
    latest_match_departments
    .select(
        "id",
        "budget",
        "budget_whole",
        "employee_level",
        "employee_salary",
        F.percent_rank().over(w3).alias("employee_salary_percent_rank"),
        F.lag("employee_salary", 1, 0).over(w4).alias("prev_employee_salary"),
        F.sum("budget_whole").over(w5).alias("cumulative_budget_whole"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "budget",
        "budget_whole",
        "employee_level",
        "employee_salary_percent_rank",
        "cumulative_budget_whole",
    )
)

result = (
    latest
    .rollup(
        F.when(F.col("cumulative_budget_whole") >= 319, "HIGH").otherwise("LOW").alias("cumulative_budget_whole_tier"),
    )
    .agg(
        F.sum(F.when(F.col("employee_level") == "L1", F.col("employee_salary_percent_rank"))).alias("l1_total_employee_salary_percent_rank"),
        F.size(F.collect_set("employee_level")).alias("num_employee_levels"),
        F.avg("cumulative_budget_whole").alias("avg_cumulative_budget_whole"),
        F.count_if(F.col("employee_level") == "L2").alias("l2_events"),
        F.count(F.when(F.col("employee_level") == "L3", 1)).alias("l3_rows"),
    )
)
