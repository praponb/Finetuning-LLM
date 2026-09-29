from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("hire_date"), "manager_id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("employee_salary").desc())
w3 = Window.partitionBy("manager_id")

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-03-04").cast("date")) & F.col("email").like("%@GMAIL.COM"))
    .withColumn("job_title_group", F.when(F.col("job_title").isin(["Director", "Engineer"]), "Tier 1").otherwise("Other"))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("DIRECT")))
)

joined = (
    base
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id").eqNullSafe(F.col("d.id")), "inner")
    .select("s.*", F.col("d.salary").alias("employee_salary"))
)

kept_keys = (
    joined
    .select("level", "job_title")
    .intersect(
        df_employees
        .filter(F.col("level").isin(["L5", "L1"]))
        .select("level", "job_title")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.full_name",
        "s.manager_id",
        "s.salary",
        "s.hire_date",
        "s.is_remote",
        "s.resolved_job_title",
        "s.employee_salary",
        F.col("b.email").alias("employee_email"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "full_name",
        "employee_salary",
        "employee_email",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.percent_rank().over(w2).alias("employee_salary_percent_rank"),
        F.stddev_samp("salary").over(w3).alias("manager_id_stddev_samp_salary"),
    )
)

result = (
    metrics
    .withColumn("salary_share_tier", F.when(F.col("salary_share") >= 0.78, "PREMIUM").otherwise("STANDARD"))
    .withColumn("employee_salary_percent_rank_tier", F.when(F.col("employee_salary_percent_rank") >= 0.78, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["salary_share_tier", "employee_salary_percent_rank_tier"], ["salary_share_tier"], []],
        "salary_share_tier",
        "employee_salary_percent_rank_tier",
    )
    .agg(
        F.percentile_approx("employee_salary_percent_rank", 0.9).alias("employee_salary_percent_rank_p90"),
        F.percentile_approx("employee_salary_percent_rank", [0.25, 0.5, 0.75]).alias("employee_salary_percent_rank_quartiles"),
    )
    .orderBy(F.col("salary_share_tier").asc_nulls_first())
)
