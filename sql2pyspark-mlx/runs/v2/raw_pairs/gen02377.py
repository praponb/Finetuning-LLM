from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.lower("email").like("%vy@%") & (F.col("dept_id") == 8) & F.col("full_name").isNotNull())
)

summary = (
    filtered_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)

result = (
    summary
    .select("hire_quarter", "level_list")
    .orderBy(F.col("hire_quarter").desc())
)
