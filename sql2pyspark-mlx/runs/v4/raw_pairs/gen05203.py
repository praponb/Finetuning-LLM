from pyspark.sql import functions as F

joined = (
    df_employees
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.full_name").alias("employee_full_name"))
)

base = (
    joined
    .filter(F.col("salary") <= 133000)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        F.concat_ws("/", "full_name", "email").alias("full_name_email_key"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

valid = (
    base
    .filter(~F.col("level").isin(["L4", "L1", "L3"]))
    .withColumn("clean_level", F.trim("level"))
)

kept_keys = (
    valid
    .select("id", "job_title")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([12, 13, 20, 30]))
        .select("id", "job_title")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

joined_employees = (
    reconciled
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.dept_name"), F.col("d.location"))
)

result = (
    joined_employees
    .cube("level", "dept_name")
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
        F.count("*").alias("event_count"),
        F.grouping("level").alias("grouping_id"),
    )
)
