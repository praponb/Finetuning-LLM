from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level")
w6 = Window.partitionBy("level").orderBy("employee_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("level").orderBy("employee_salary", "id")
w_grp = Window.partitionBy("level").orderBy("running_employee_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("level").orderBy("prev_employee_salary", "id")
w10 = Window.partitionBy("running_employee_salary_tier").orderBy(F.col("event_count").desc())

latest = (
    df_departments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_latest = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.budget", F.col("c.hire_date"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= F.col("s.hire_date")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.hire_date",
        F.col("c.level"),
        F.col("c.salary").alias("employee_salary"),
    )
)

enriched = (
    with_last
    .filter(F.col("level").isin(["L3", "L2", "L1"]))
    .select(
        "id",
        "level",
        F.sum("employee_salary").over(w4).alias("running_employee_salary"),
        F.round((F.col("employee_salary") / F.sum("employee_salary").over(w5)), 4).alias("employee_salary_share"),
        F.first("level", ignorenulls=True).over(w6).alias("first_known_level"),
        F.round((F.col("employee_salary") / F.sum("employee_salary").over(w5)), 4).alias("employee_salary_share_2"),
        F.lag("employee_salary", 1, 0).over(w7).alias("prev_employee_salary"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "level",
        "running_employee_salary",
        "employee_salary_share",
        "first_known_level",
        "prev_employee_salary",
        F.last("employee_salary_share", ignorenulls=True).over(w_grp).alias("last_known_employee_salary_share"),
        F.row_number().over(w9).alias("row_seq"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("running_employee_salary") >= 681399, "LARGE").otherwise("SMALL").alias("running_employee_salary_tier"),
        "level",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("employee_salary_share", 0.5).alias("employee_salary_share_p50"),
        F.percentile_approx("running_employee_salary", [0.25, 0.5, 0.75]).alias("running_employee_salary_quartiles"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
    )
    .withColumn("event_count_percent_rank", F.percent_rank().over(w10))
    .orderBy("l1_count")
)
