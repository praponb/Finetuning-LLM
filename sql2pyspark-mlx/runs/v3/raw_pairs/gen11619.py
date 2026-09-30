from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("salary").between(67000, 127000))
    .groupBy("level")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        F.min("salary").alias("min_salary"),
    )
)
