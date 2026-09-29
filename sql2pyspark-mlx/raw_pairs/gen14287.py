from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%IM %"))
    .groupBy("level")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
        F.min("salary").alias("min_salary"),
        F.sum("salary").alias("total_salary"),
    )
)
