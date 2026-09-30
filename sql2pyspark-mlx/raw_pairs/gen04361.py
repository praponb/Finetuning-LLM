from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(~(F.col("manager_id").isin([8, 24, 27, 34]) & (F.col("hire_date") <= "2024-07-13")))
    .withColumn("clean_job_title", F.trim("job_title"))
)

employees_stats = (
    recent_employees
    .groupBy("level")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
    )
)

result = (
    employees_stats
    .filter(F.col("l2_count") < 35)
    .select("level", "l2_count")
)
