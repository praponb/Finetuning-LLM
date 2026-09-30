from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc())
w_recent = Window.partitionBy("dept_name").orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("dept_name").orderBy("employee_salary", "id")
w6 = Window.partitionBy("dept_name", "location").orderBy(F.col("first_known_budget").desc_nulls_first(), "id")

deduped = (
    df_departments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_last = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.salary").alias("employee_salary"),
    )
)

scored = (
    with_last
    .withColumn("budget_percent_rank", F.percent_rank().over(w3))
    .withColumn("first_known_budget", F.first("budget", ignorenulls=True).over(w_recent))
    .withColumn("prev_employee_salary", F.lag("employee_salary", 2, 0).over(w_hist))
)

kept = (
    scored
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") >= F.col("s.employee_hire_date")), "left_semi")
)

deduped_departments = (
    kept
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

matched_keys = (
    deduped_departments
    .select("dept_name", "id")
    .intersect(
        df_departments
        .filter(F.col("budget") > 1178000)
        .select("dept_name", "id")
    )
)

verified = (
    deduped_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("week", "employee_hire_date").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
        F.max(F.when(F.col("dept_name") == "Engineering", F.col("budget"))).alias("engineering_max_budget"),
        F.countDistinct("location").alias("distinct_locations"),
    )
    .orderBy(F.col("engineering_max_budget").asc_nulls_first())
)
