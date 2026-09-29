from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("dept_id", "location")
w3 = Window.partitionBy("job_title").orderBy("budget", "id")

valid = (
    df_employees
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-05-31")))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
    .withColumn("full_name_length", F.length("full_name"))
)

with_dims = (
    valid
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.budget"), F.col("d.location"), F.col("d.dept_name"))
)

cleaned = (
    with_dims
    .filter(F.col("hire_date").between("2023-10-01", "2025-03-01"))
    .withColumn("salary_minus_is_remote_flag", (F.col("salary") - F.col("is_remote_flag")))
    .withColumn("dept_name_job_title", F.concat("dept_name", F.lit("/"), "job_title"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w).alias("budget_cume_dist"),
        F.avg("salary").over(w2).alias("dept_id_avg_salary"),
        F.lag("is_remote_flag", 2).over(w3).alias("prev_is_remote_flag"),
    )
)

kept_keys = (
    enriched
    .select("manager_id", "id")
    .intersectAll(
        df_employees
        .filter(F.col("email").like("%@startup.io"))
        .select("manager_id", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.dept_name").alias("department_dept_name"))
)

enriched_employees = (
    joined
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.dept_name").alias("department_dept_name_2"),
        F.col("d.budget").alias("department_budget"),
        F.col("d.location").alias("department_location"),
    )
)

result = (
    enriched_employees
    .withColumn("prev_is_remote_flag_tier", F.when(F.col("prev_is_remote_flag") >= 61, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("WEEK", "hire_date"))
    .cube("prev_is_remote_flag_tier", "week_start")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("salary_minus_is_remote_flag")).otherwise(0)).alias("engineering_salary_minus_is_remote_flag"),
        F.sum(F.when(F.col("location") == "Berlin", F.col("prev_is_remote_flag")).otherwise(0)).alias("berlin_prev_is_remote_flag"),
        F.percentile_approx("salary_minus_is_remote_flag", [0.25, 0.5, 0.75]).alias("salary_minus_is_remote_flag_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("engineering_salary_minus_is_remote_flag").desc())
)
