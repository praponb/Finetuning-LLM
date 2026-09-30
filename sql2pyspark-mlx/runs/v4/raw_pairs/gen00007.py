from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("dept_id").orderBy("is_remote_flag", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("level").orderBy("is_remote_flag")
w4 = Window.orderBy("least_manager_id_max_salary_salary_avg_5_prior")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-28").cast("date")) & (F.col("level") == "L2"))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("N/A")))
)

enriched = (
    cleaned
    .withColumn("manager_id_max_salary", F.max("salary").over(w))
    .withColumn("salary_avg_5_prior", F.avg("salary").over(w2))
)

kept_keys = (
    enriched
    .select("manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("email").isNull())
        .select("manager_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("salary_avg_5_prior") > 94063)
    .withColumn("least_manager_id_max_salary_salary_avg_5_prior", F.least("manager_id_max_salary", "salary_avg_5_prior"))
)

enriched_employees = (
    valid
    .select(
        "*",
        F.rank().over(w3).alias("is_remote_flag_rank"),
        F.cume_dist().over(w4).alias("least_manager_id_max_salary_salary_avg_5_prior_cume_dist"),
    )
)

joined = (
    enriched_employees
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.location"), F.col("d.budget"))
)

daily = (
    joined
    .groupBy(
        F.when(F.col("salary") >= 154000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("least_manager_id_max_salary_salary_avg_5_prior", [0.25, 0.5, 0.75]).alias("least_manager_id_max_salary_salary_avg_5_prior_quartiles"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_rows"),
        F.percentile_approx("manager_id_max_salary", [0.25, 0.5, 0.75]).alias("manager_id_max_salary_quartiles"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
    )
)

result = (
    daily
    .select(
        "salary_tier",
        "year_start",
        "tokyo_rows",
        "manager_id_max_salary_quartiles",
        "analyst_count",
    )
    .orderBy("year_start", "salary_tier")
)
