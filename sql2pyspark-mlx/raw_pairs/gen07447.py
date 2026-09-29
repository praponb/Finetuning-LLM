from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id")
w2 = Window.partitionBy("location", "level").orderBy(F.col("salary").desc(), "id")
w3 = Window.orderBy("salary_bucket", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy(F.year("hire_date"), "location").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_employees
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "left")
    .filter(F.col("b.budget").between(2607000, 3174000))
    .select("src.*", F.col("b.location"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id").eqNullSafe(F.col("e.id")), "left")
    .select("s.*", F.col("e.job_title").alias("employee_job_title"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.lead("salary", 2).over(w).alias("next_salary"),
        F.ntile(4).over(w2).alias("salary_bucket"),
    )
)

with_dims_employees = (
    enriched
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.email",
        "src.dept_id",
        "src.hire_date",
        "src.location",
        "src.employee_job_title",
        "src.salary_bucket",
        F.col("d.location").alias("department_location"),
        F.col("d.budget"),
    )
)

metrics = (
    with_dims_employees
    .withColumn("salary_bucket_avg_5_prior", F.avg("salary_bucket").over(w3))
    .withColumn("running_salary_bucket", F.sum("salary_bucket").over(w4))
)

result = (
    metrics
    .withColumn("year_start", F.date_trunc("YEAR", "hire_date"))
    .withColumn("activity_month", F.date_trunc("MONTH", "hire_date"))
    .groupingSets(
        [["year_start", "activity_month"], ["year_start"]],
        "year_start",
        "activity_month",
    )
    .agg(
        F.count(F.when(F.col("employee_job_title") == "Analyst", 1)).alias("analyst_rows"),
        F.percentile_approx("salary_bucket_avg_5_prior", [0.25, 0.5, 0.75]).alias("salary_bucket_avg_5_prior_quartiles"),
        F.round(F.sum("salary_bucket_avg_5_prior") / F.nullif(F.count(F.when(F.col("location") == "Tokyo", 1)), F.lit(0)), 2).alias("salary_bucket_avg_5_prior_per_tokyo"),
        F.count_if(F.col("employee_job_title") == "Designer").alias("designer_events"),
        F.sort_array(F.collect_set("employee_job_title")).alias("employee_job_title_list"),
        F.grouping("year_start").alias("grouping_id"),
    )
    .filter(F.col("salary_bucket_avg_5_prior_per_tokyo") < 1000000)
)
