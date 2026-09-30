from pyspark.sql import functions as F

base = (
    df_employees
    .filter(~F.col("is_remote") & ((F.col("job_title") != "Analyst") | (F.col("salary") >= 57000)))
)

summary = (
    base
    .groupBy("is_remote")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
    )
)

result = (
    summary
    .filter(F.col("pct_analyst") < 54)
    .select("is_remote", "latest_hire_date", "pct_analyst")
    .orderBy("latest_hire_date")
)
