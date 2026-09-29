from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("manager_id").orderBy(F.col("months_since_hire_date").desc_nulls_last())
w3 = Window.partitionBy("week_start").orderBy(F.col("designer_events").desc())

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-08-26").cast("date")) & F.col("manager_id").isNotNull())
    .withColumn("is_director", F.when(F.col("job_title") == "Director", 1).otherwise(0))
)

with_dims = (
    filtered
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.dept_id",
        "s.manager_id",
        "s.salary",
        "s.hire_date",
        F.col("d.budget"),
        F.col("d.dept_name"),
    )
)

filtered_employees = (
    with_dims
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "hire_date",
        "budget",
        "dept_name",
        F.round("budget", 1).alias("budget_rounded"),
        (F.datediff(F.current_date(), "hire_date") / 30).alias("months_since_hire_date"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"),
    )
)

base = (
    filtered_employees
    .filter(F.col("budget").isNotNull() & (F.col("dept_name").eqNullSafe(F.lit("Legal"))))
    .withColumn("budget_rounded_per_months_since_hire_date", (F.col("budget_rounded") / F.nullif("months_since_hire_date", F.lit(0))))
    .withColumn("months_since_hire_date_minus_budget_rounded", (F.col("months_since_hire_date") - F.col("budget_rounded")))
)

enriched = (
    base
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.manager_id",
        "e.hire_date",
        "e.budget",
        "e.dept_name",
        "e.months_since_hire_date",
        "e.dept_name_filled",
        F.col("p.email"),
        F.col("p.job_title"),
        F.col("p.full_name"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.email").alias("employee_email"))
)

scored = (
    joined
    .select(
        "*",
        F.last("dept_name", ignorenulls=True).over(w).alias("last_known_dept_name"),
        F.cume_dist().over(w_hist).alias("months_since_hire_date_cume_dist"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
        F.when(F.col("budget") >= 3518000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
        F.min("months_since_hire_date").alias("min_months_since_hire_date"),
        F.max("months_since_hire_date").alias("max_months_since_hire_date"),
        F.count_if(F.col("job_title") == "Designer").alias("designer_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("months_since_hire_date", [0.25, 0.5, 0.75]).alias("months_since_hire_date_quartiles"),
    )
    .withColumn("designer_events_rank", F.rank().over(w3))
)
