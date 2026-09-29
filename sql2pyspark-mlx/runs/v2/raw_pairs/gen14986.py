from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.sum(F.when(F.col("e.job_title") == "Designer", 1).otherwise(0)).alias("designer_count"),
        F.sort_array(F.collect_set(F.col("e.level"))).alias("level_list"),
    )
    .filter(F.col("employee_count") >= 3)
)
