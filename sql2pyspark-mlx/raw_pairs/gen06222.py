from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("id", "level")
    .exceptAll(
        df_employees
        .filter(~F.col("is_remote"))
        .select("id", "level")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id").eqNullSafe(F.col("e.id")), "left")
    .filter(F.col("e.full_name").isNull())
    .select("a.*", F.col("e.hire_date").alias("employee_hire_date"))
)

joined = (
    enriched
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.full_name",
        "src.email",
        "src.dept_id",
        "src.manager_id",
        "src.level",
        "src.employee_hire_date",
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
    )
)

filtered = (
    joined
    .filter(F.col("level").isNotNull() & (F.col("location") != "Tokyo"))
    .withColumn("level_lower", F.lower("level"))
    .withColumn("clean_full_name", F.trim("full_name"))
)

result = (
    filtered
    .groupBy(
        F.date_trunc("MONTH", "employee_hire_date").alias("activity_month"),
        "dept_id",
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
        F.count_if(F.col("dept_name") == "HR").alias("hr_events"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_rows"),
        F.sum("budget").alias("total_budget"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
)
