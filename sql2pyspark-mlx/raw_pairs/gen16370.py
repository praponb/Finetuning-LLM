from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("salary").desc())
w3 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("job_title").orderBy("salary", "id")

cleaned = (
    df_employees
    .filter(F.col("dept_id").isin([26, 39, 40]) & (F.col("hire_date") >= F.to_date(F.lit("2023-08-01"))))
    .select(
        "*",
        F.trim("level").alias("clean_level"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

enriched = (
    cleaned
    .filter(F.col("manager_id") == 13)
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "level_filled",
        F.sum("salary").over(w).alias("running_salary"),
        F.cume_dist().over(w2).alias("salary_cume_dist"),
        F.sum("salary").over(w3).alias("running_salary_2"),
        F.ntile(3).over(w_ordered).alias("salary_bucket"),
    )
)

cleaned_employees = (
    enriched
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-05-14"))) & F.col("manager_id").isNotNull())
    .withColumn("running_salary_2_rounded", F.round("running_salary_2", 0))
    .withColumn("running_salary_2_filled", F.coalesce("running_salary_2", F.lit(0)))
)

matched_keys = (
    cleaned_employees
    .select("job_title", "manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("manager_id").isin([18, 25, 30]))
        .select("job_title", "manager_id")
    )
)

verified = (
    cleaned_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("hire_date") >= F.lit("2023-07-30").cast("date"))
    .withColumn("full_name_prefix", F.substring("full_name", 1, 2))
    .withColumn("hire_date_minus_47d", F.date_sub("hire_date", 47))
)

result = (
    filtered
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .groupingSets(
        [["level", "employee_job_title"], ["level"]],
        F.col("p.level").alias("level"),
        F.col("e.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("e.running_salary_2_rounded")), 2).alias("total_running_salary_2_rounded"),
        F.min(F.col("e.salary_cume_dist")).alias("min_salary_cume_dist"),
        F.grouping_id().alias("gid"),
    )
)
