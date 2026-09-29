from pyspark.sql import functions as F

enriched = (
    df_employees
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.email",
        "e.dept_id",
        "e.manager_id",
        "e.job_title",
        "e.is_remote",
        "e.level",
        F.col("d.level").alias("employee_level"),
    )
)

result = (
    enriched
    .groupBy("employee_level")
    .agg(
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("employee_level") == "L4", 1)).alias("l4_count"),
        F.count_if(F.col("employee_level") == "L4").alias("l4_events"),
    )
    .filter(F.col("num_levels") >= 1)
    .orderBy(F.col("employee_level").asc_nulls_last())
)
