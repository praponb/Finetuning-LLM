from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "level")
w2 = Window.partitionBy("dept_id", "manager_id")
w3 = Window.partitionBy("job_title").orderBy("budget", "id").rowsBetween(-6, -1)

base = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2024-01-15").cast("date"))
    .select("*", F.date_trunc("week", "hire_date").alias("hire_date_period"))
)

joined = (
    base
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.dept_name"), F.col("p.budget"), F.col("p.location"))
)

with_windows = (
    joined
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w)), 4))
    .withColumn("budget_share_2", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("salary_avg_6_prior", F.avg("salary").over(w3))
)

totals = (
    with_windows
    .groupBy(
        F.when(F.col("budget") >= 4193000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.countDistinct("location").alias("distinct_locations"),
        F.avg("salary_avg_6_prior").alias("avg_salary_avg_6_prior"),
        F.round(F.avg("budget_share"), 2).alias("avg_budget_share"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    totals
    .select("budget_tier", "avg_salary_avg_6_prior", "num_rows")
    .orderBy(
        F.col("budget_tier").asc_nulls_first(),
        F.col("avg_salary_avg_6_prior").desc(),
    )
)
