from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("location", "dept_name").orderBy("last3_sum_budget", "id").rowsBetween(-6, Window.currentRow)
w_part_2 = Window.partitionBy("location").orderBy("last3_max_budget", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.year("employee_hire_date"), "location").orderBy("last3_min_budget", "id")
w6 = Window.partitionBy("dept_name").orderBy("prev_last3_sum_budget", "id")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_lower", F.lower("dept_name"))
)

metrics = (
    cleaned
    .select(
        "id",
        "dept_name",
        "location",
        F.sum("budget").over(w).alias("last3_sum_budget"),
        F.min("budget").over(w).alias("last3_min_budget"),
        F.max("budget").over(w).alias("last3_max_budget"),
        F.count("budget").over(w).alias("last3_count_budget"),
    )
)

scored = (
    metrics
    .filter(F.col("last3_min_budget") > 2296000)
    .select(
        "*",
        F.max("last3_count_budget").over(w_part).alias("last7_max_last3_count_budget"),
        F.min("last3_count_budget").over(w_part).alias("last7_min_last3_count_budget"),
        F.count("last3_count_budget").over(w_part).alias("last7_count_last3_count_budget"),
        F.lag("last3_sum_budget", 2).over(w_part_2).alias("prev_last3_sum_budget"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

metrics_departments = (
    with_latest
    .withColumn("next_prev_last3_sum_budget", F.lead("prev_last3_sum_budget", 2, 0).over(w5))
    .withColumn("next_last7_count_last3_count_budget", F.lead("last7_count_last3_count_budget", 1).over(w6))
)

by_type = (
    metrics_departments
    .groupBy("location")
    .pivot("dept_name", ["Legal", "HR"])
    .agg(F.max("last7_max_last3_count_budget"))
)

result = (
    by_type
    .select("location", "Legal")
    .orderBy(F.col("Legal").asc_nulls_first(), F.col("location").desc())
)
