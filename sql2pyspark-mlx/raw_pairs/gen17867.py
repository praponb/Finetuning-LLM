from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("dept_name", "employee_job_title").orderBy("budget", "id")
w3 = Window.partitionBy("employee_level", "location").orderBy("budget", "id")
w4 = Window.partitionBy("location", "employee_level")
w5 = Window.partitionBy(F.date_trunc("MONTH", "employee_hire_date"), "location").orderBy("employee_hire_date", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy(F.month("employee_hire_date")).orderBy(F.col("budget").desc_nulls_first())

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.upper("location").alias("location_upper"),
        F.when(F.col("budget") >= 3443000, "High").when(F.col("budget") >= 1646000, "Medium").otherwise("Low").alias("budget_band"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.level").alias("employee_level"),
    )
)

metrics = (
    latest_match
    .select(
        "id",
        "location",
        "budget",
        "location_upper",
        "employee_job_title",
        "employee_level",
        F.lag("budget").over(w_grp).alias("prev_budget"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        "s.location_upper",
        "s.employee_level",
        F.col("c.job_title"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

scored = (
    with_last
    .select(
        "id",
        "budget",
        "employee_level",
        "job_title",
        "employee_hire_date",
        F.lead("budget", 1).over(w3).alias("next_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share"),
        F.avg("budget").over(w5).alias("budget_avg_3"),
        F.dense_rank().over(w6).alias("budget_dense_rank"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") < F.col("s.employee_hire_date")), "left_semi")
)

kept_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") >= F.col("s.employee_hire_date")), "left_anti")
)

result = (
    kept_departments
    .select(
        "id",
        "budget",
        "employee_level",
        "employee_hire_date",
        "next_budget",
        "budget_dense_rank",
        F.when(F.col("employee_level").rlike("^(L2|L4)$"), "PRIORITY").when(F.col("employee_level").rlike("^(L1|L3)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("employee_level_zone"),
    )
    .orderBy(F.col("budget_dense_rank").desc_nulls_last())
)
