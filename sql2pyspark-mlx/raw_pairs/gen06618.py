from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("level").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy("is_legal", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("running_sum_is_legal", "id")
w5 = Window.partitionBy("dept_name").orderBy(F.col("employee_salary").desc(), "id")
w6 = Window.partitionBy("dept_name")
w7 = Window.orderBy("running_avg_is_legal", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("dept_name").orderBy("running_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("dept_name").orderBy("running_budget", "id").rowsBetween(-6, -1)

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("location").eqNullSafe(F.lit("London"))))
    .withColumn("location_group", F.when(F.col("location").isin(["Singapore", "Chiang Mai"]), "Group A").otherwise("Other"))
    .withColumn("location_clean", F.coalesce("location", F.lit("N/A")))
)

filtered_departments = (
    filtered
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Marketing")))
    .withColumn("is_legal", F.when(F.col("dept_name") == "Legal", 1).otherwise(0))
)

latest_match = (
    filtered_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"))
)

with_windows = (
    latest_match
    .withColumn("running_max_is_legal", F.max("is_legal").over(w_grp))
    .withColumn("running_avg_is_legal", F.avg("is_legal").over(w_grp))
    .withColumn("running_sum_is_legal", F.sum("is_legal").over(w_grp))
    .withColumn("running_budget", F.sum("budget").over(w3))
)

latest_match_departments = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget",
        "s.location_group",
        "s.location_clean",
        "s.running_max_is_legal",
        "s.running_avg_is_legal",
        "s.running_sum_is_legal",
        "s.running_budget",
        F.col("c.salary").alias("employee_salary"),
    )
)

scored = (
    latest_match_departments
    .filter(F.col("employee_salary").isNotNull())
    .select(
        "id",
        "dept_name",
        "location_clean",
        "running_max_is_legal",
        "running_budget",
        F.lead("employee_salary", 2).over(w4).alias("next_employee_salary"),
        F.ntile(10).over(w5).alias("employee_salary_bucket"),
        F.count("running_budget").over(w6).alias("dept_name_count_running_budget"),
        F.avg("running_sum_is_legal").over(w7).alias("running_sum_is_legal_avg_5_prior"),
    )
)

with_windows_departments = (
    scored
    .select(
        "id",
        "location_clean",
        "next_employee_salary",
        "dept_name_count_running_budget",
        "running_sum_is_legal_avg_5_prior",
        F.avg("running_sum_is_legal_avg_5_prior").over(w8).alias("running_avg_running_sum_is_legal_avg_5_prior"),
        F.sum("running_sum_is_legal_avg_5_prior").over(w8).alias("running_sum_running_sum_is_legal_avg_5_prior"),
        F.max("running_sum_is_legal_avg_5_prior").over(w8).alias("running_max_running_sum_is_legal_avg_5_prior"),
        F.avg("next_employee_salary").over(w9).alias("next_employee_salary_avg_6_prior"),
    )
)

result = (
    with_windows_departments
    .groupBy(
        F.when(F.col("running_avg_running_sum_is_legal_avg_5_prior") >= 360, "LARGE").otherwise("SMALL").alias("running_avg_running_sum_is_legal_avg_5_prior_tier"),
        F.when(F.col("dept_name_count_running_budget") >= 15, "PREMIUM").otherwise("STANDARD").alias("dept_name_count_running_budget_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("running_sum_is_legal_avg_5_prior", [0.25, 0.5, 0.75]).alias("running_sum_is_legal_avg_5_prior_quartiles"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("dept_name_count_running_budget_tier").desc_nulls_first())
)
