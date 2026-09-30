from pyspark.sql import functions as F

src = (
    df_employees
    .filter((F.col("job_title") == "Director") & F.col("job_title").isNotNull())
)

employees_stats = (
    src
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    employees_stats
    .select("job_title", "engineer_count", "level_list")
    .orderBy("job_title")
    .limit(5)
)
