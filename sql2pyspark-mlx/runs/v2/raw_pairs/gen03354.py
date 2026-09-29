from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max("hire_date").alias("latest_hire_date"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineer"),
    )
    .orderBy(F.col("avg_salary").desc())
)
