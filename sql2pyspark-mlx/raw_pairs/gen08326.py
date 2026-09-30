from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("location").orderBy("adjusted_budget", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("budget") < 3261000)
    .select(
        "*",
        F.substring("location", 1, 3).alias("location_prefix"),
        F.round((F.col("budget") * 1.15), 2).alias("adjusted_budget"),
        F.coalesce("location", F.lit("N/A")).alias("resolved_location"),
    )
)

valid = (
    filtered
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "location",
        "location_prefix",
        "adjusted_budget",
        F.when(F.col("dept_name").isin(["Engineering", "Sales"]), "Group A").otherwise("Other").alias("dept_name_group"),
        F.trim("dept_name").alias("clean_dept_name"),
        F.coalesce("location", F.lit("OTHER")).alias("location_filled"),
    )
)

enriched = (
    valid
    .filter(F.col("adjusted_budget") >= 53)
    .withColumn("last4_sum_adjusted_budget", F.sum("adjusted_budget").over(w_grp))
    .withColumn("last4_max_adjusted_budget", F.max("adjusted_budget").over(w_grp))
    .withColumn("last4_count_adjusted_budget", F.count("adjusted_budget").over(w_grp))
    .withColumn("last4_avg_adjusted_budget", F.avg("adjusted_budget").over(w_grp))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") >= F.col("s.employee_hire_date")), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.dept_name_group",
        "s.location_filled",
        "s.last4_sum_adjusted_budget",
        "s.last4_count_adjusted_budget",
        "s.last4_avg_adjusted_budget",
        F.col("c.job_title"),
    )
)

result = (
    with_last
    .select(
        "id",
        "location",
        "dept_name_group",
        "location_filled",
        "last4_count_adjusted_budget",
        "last4_avg_adjusted_budget",
        "job_title",
    )
    .orderBy(F.col("last4_avg_adjusted_budget").desc())
)
