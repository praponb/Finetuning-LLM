from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%ARO%") & (F.col("salary") >= 58000))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
    )
)
