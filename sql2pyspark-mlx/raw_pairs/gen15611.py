from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

result = (
    with_latest
    .groupBy(F.date_trunc("month", "employee_hire_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_rows"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget"))).alias("chiang_mai_total_budget"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy(F.col("hr_count").desc())
)
