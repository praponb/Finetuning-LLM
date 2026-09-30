from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("employee_hire_date"), "job_title").orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location")
w_ordered = Window.partitionBy("dept_name").orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy(F.month("employee_hire_date"), "location").orderBy("employee_hire_date", "id").rowsBetween(-3, -1)
w7 = Window.orderBy(F.col("budget").desc_nulls_first())
w8 = Window.partitionBy(F.year("employee_hire_date"), "location").orderBy(F.col("budget").desc(), "id")
w9 = Window.partitionBy(F.year("employee_hire_date"), "dept_name").orderBy("cumulative_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("location").orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "Sales", "Marketing"]) & F.col("location").isNotNull())
    .withColumn("location_prefix", F.substring("location", 1, 3))
    .withColumn("location_filled", F.coalesce("location", F.lit("UNKNOWN")))
)

latest_match = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

scored = (
    latest_match
    .withColumn("cumulative_budget", F.sum("budget").over(w2))
    .withColumn("cumulative_budget_2", F.sum("budget").over(w3))
    .withColumn("location_avg_budget", F.avg("budget").over(w4))
    .withColumn("running_budget", F.sum("budget").over(w_ordered))
    .withColumn("budget_avg_3_prior", F.avg("budget").over(w_grp))
)

enriched = (
    scored
    .select(
        "id",
        "location",
        "location_filled",
        "job_title",
        "employee_hire_date",
        "cumulative_budget",
        "budget_avg_3_prior",
        F.cume_dist().over(w7).alias("budget_cume_dist"),
        F.row_number().over(w8).alias("row_seq"),
        F.sum("budget").over(w9).alias("running_budget_2"),
    )
)

scored_departments = (
    enriched
    .filter(F.col("row_seq") == 8)
    .withColumn("running_sum_row_seq", F.sum("row_seq").over(w_running))
    .withColumn("running_count_row_seq", F.count("row_seq").over(w_running))
    .withColumn("running_avg_row_seq", F.avg("row_seq").over(w_running))
    .withColumn("running_max_row_seq", F.max("row_seq").over(w_running))
)

by_type = (
    scored_departments
    .groupBy("job_title")
    .pivot("location", ["Singapore", "Berlin"])
    .agg(F.max("budget_avg_3_prior"))
    .select(
        "job_title",
        F.col("Singapore").alias("singapore"),
        F.col("Berlin").alias("berlin"),
    )
)

result = (
    by_type
    .select("job_title", "singapore", "berlin")
    .orderBy("singapore")
)
