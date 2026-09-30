from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") == "Manager") & F.col("full_name").like("%Mia%") & F.col("hire_date").between("2024-11-01", "2025-08-01"))
    .groupBy("level")
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)
