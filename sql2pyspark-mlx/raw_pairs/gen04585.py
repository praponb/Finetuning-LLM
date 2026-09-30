from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("manager_id").orderBy("employee_salary", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("employee_salary", "id")
w4 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("salary_avg_2_prior_2", "id")

valid = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.lower("full_name").alias("full_name_lower"),
        F.coalesce("level", F.lit("Unknown")).alias("level_filled"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_filled"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "inner")
    .select(
        "a.id",
        "a.manager_id",
        "a.job_title",
        "a.salary",
        "a.is_remote",
        "a.level_filled",
        "a.job_title_filled",
        F.col("e.salary").alias("employee_salary"),
        F.col("e.level").alias("employee_level"),
    )
)

metrics = (
    with_dims
    .filter(F.col("manager_id").isin([29, 38]))
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_2_prior"),
        F.avg("salary").over(w2).alias("salary_avg_2_prior_2"),
        F.lead("salary", 1).over(w3).alias("next_salary"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "manager_id",
        "salary",
        "level_filled",
        "employee_salary",
        "salary_avg_2_prior_2",
        "next_salary",
        F.avg("salary").over(w4).alias("running_avg_salary"),
        F.max("salary").over(w4).alias("running_max_salary"),
        F.min("salary").over(w4).alias("running_min_salary"),
        F.sum("salary").over(w4).alias("running_sum_salary"),
        F.lag("next_salary", 1, 0).over(w5).alias("prev_next_salary"),
    )
)

valid_employees = (
    scored
    .filter(F.col("manager_id").isNotNull())
    .select(
        "*",
        F.when(F.col("employee_salary") >= 156000, "High").when(F.col("employee_salary") >= 48000, "Medium").otherwise("Low").alias("employee_salary_level"),
        F.round("salary_avg_2_prior_2", 2).alias("salary_avg_2_prior_2_rounded"),
    )
)

enriched = (
    valid_employees
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.manager_id",
        "s.level_filled",
        "s.employee_salary",
        "s.salary_avg_2_prior_2",
        "s.running_avg_salary",
        "s.running_max_salary",
        "s.running_min_salary",
        "s.running_sum_salary",
        "s.prev_next_salary",
        F.col("d.job_title"),
    )
)

result = (
    enriched
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "left")
    .cube(
        F.col("p.job_title").alias("employee_job_title"),
        F.col("src.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("src.salary_avg_2_prior_2")), 2).alias("avg_salary_avg_2_prior_2"),
        F.max(F.col("src.prev_next_salary")).alias("max_prev_next_salary"),
    )
)
