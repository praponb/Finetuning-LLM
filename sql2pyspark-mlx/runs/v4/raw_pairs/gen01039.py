from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("level").isin(["L4", "L1"]))

agg = (
    valid_employees
    .groupBy("job_title", "manager_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
    )
)

result = (
    agg
    .filter(F.col("pct_designer") > 78)
    .select("job_title", "pct_designer", "analyst_count")
    .orderBy("analyst_count")
)
