from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("budget") >= 1517000))
    .withColumn("dept_name_length", F.length("dept_name"))
)

eligible = (
    filtered
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") <= "2025-01-14"), "left_anti")
)

matched_keys = (
    eligible
    .select("location", "id")
    .subtract(
        df_departments
        .filter(F.col("location") != "Tokyo")
        .select("location", "id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.job_title").isin(["Manager", "Designer"])), "left_semi")
)

eligible_departments = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.email").like("%@gmail.com")), "left_anti")
)

result = (
    eligible_departments
    .select("id", "dept_name")
    .orderBy("id")
)
