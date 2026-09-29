from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([1, 25, 31]))
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("pct_designer") < 68)
    .orderBy(F.col("pct_designer").desc())
)
