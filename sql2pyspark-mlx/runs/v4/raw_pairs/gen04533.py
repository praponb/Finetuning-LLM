from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "job_title").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("job_title").orderBy("salary", "id")
w3 = Window.partitionBy("job_title", "dept_id").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("hire_date", "id")

filtered = (
    df_employees
    .filter(F.col("hire_date") > "2024-08-01")
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.lower("level").alias("level_lower"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.ntile(3).over(w).alias("salary_bucket"),
        F.ntile(3).over(w2).alias("salary_bucket_2"),
        F.avg("salary").over(w3).alias("salary_avg_2"),
        F.lead("salary", 2).over(w4).alias("next_salary"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.location"))
)

stats = (
    joined
    .groupBy(
        F.when(F.col("salary_avg_2") >= 122923, "HIGH").otherwise("LOW").alias("salary_avg_2_tier"),
    )
    .agg(
        F.percentile_approx("next_salary", [0.25, 0.5, 0.75]).alias("next_salary_quartiles"),
        F.percentile_approx("salary_bucket", 0.9).alias("salary_bucket_p90"),
        F.max(F.when(F.col("job_title") == "Designer", F.col("salary_avg_2"))).alias("designer_max_salary_avg_2"),
    )
)

result = (
    stats
    .select(
        "salary_avg_2_tier",
        "next_salary_quartiles",
        "salary_bucket_p90",
        "designer_max_salary_avg_2",
    )
    .orderBy(F.col("salary_bucket_p90").desc(), "designer_max_salary_avg_2")
)
