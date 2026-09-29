from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary")
w2 = Window.partitionBy("manager_id")
w3 = Window.orderBy("salary")
w4 = Window.partitionBy("dept_id", "job_title")
w5 = Window.partitionBy("level")
w6 = Window.partitionBy("manager_id").orderBy("manager_id_avg_salary")
w7 = Window.partitionBy("dept_name").orderBy("salary_dense_rank")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-26").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.when(F.col("level").isin(["L3", "L2"]), "Tier 1").otherwise("Other").alias("level_group"),
        F.coalesce("email", F.lit("N/A")).alias("email_filled"),
        F.coalesce("job_title", F.lit("OTHER")).alias("resolved_job_title"),
    )
)

remaining_keys = (
    valid
    .select("level", "manager_id")
    .intersect(
        df_employees
        .filter(F.col("is_remote"))
        .select("level", "manager_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.avg("salary").over(w2).alias("manager_id_avg_salary"),
        F.dense_rank().over(w3).alias("salary_dense_rank_2"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.sum("manager_id_avg_salary").over(w4).alias("dept_id_sum_manager_id_avg_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w5)), 4).alias("salary_share"),
        F.round((F.col("manager_id_avg_salary") / F.sum("manager_id_avg_salary").over(w5)), 4).alias("manager_id_avg_salary_share"),
        F.rank().over(w6).alias("manager_id_avg_salary_rank"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.dept_name"), F.col("d.budget"), F.col("d.location"))
)

enriched_employees = (
    enriched
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "inner")
    .select(
        "s.*",
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.email").alias("employee_email"),
        F.col("e.job_title").alias("employee_job_title"),
    )
)

enriched_employees_2 = (
    enriched_employees
    .withColumn("salary_dense_rank_dense_rank", F.dense_rank().over(w7))
    .withColumn("level_sum_budget", F.sum("budget").over(w5))
)

result = (
    enriched_employees_2
    .groupBy(F.date_trunc("week", "hire_date").alias("week_start"))
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.sort_array(F.collect_set("employee_job_title")).alias("employee_job_title_list"),
    )
)
