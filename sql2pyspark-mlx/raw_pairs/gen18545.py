from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.orderBy(F.col("salary").desc())

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-02-22").cast("date")) & (F.col("level") == "L3"))
    .withColumn("days_since_hire_date", F.datediff(F.current_date(), "hire_date"))
    .withColumn("clean_level", F.trim("level"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("OTHER")))
)

enriched = (
    base
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.level").alias("employee_level"),
        F.col("d.email").alias("employee_email"),
        F.col("d.full_name").alias("employee_full_name"),
    )
)

kept_keys = (
    enriched
    .select("id", "level")
    .exceptAll(
        df_employees
        .filter(F.col("email").like("%@gmail.com"))
        .select("id", "level")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.rank().over(w2).alias("salary_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.lower("employee_email").like("%mma%"))
    .withColumn("full_name_prefix", F.substring("full_name", 1, 2))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("NONE")))
)

joined = (
    filtered
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") < 3910000)
    .select(
        "s.full_name",
        "s.email",
        "s.dept_id",
        "s.salary",
        "s.hire_date",
        "s.is_remote",
        "s.level",
        "s.days_since_hire_date",
        "s.clean_level",
        "s.job_title_clean",
        "s.employee_email",
        "s.full_name_prefix",
        "s.job_title_filled",
        F.col("d.budget"),
    )
)

result = (
    joined
    .groupBy("dept_id")
    .pivot("level", ["L1", "L5"])
    .agg(F.max("days_since_hire_date"))
)
