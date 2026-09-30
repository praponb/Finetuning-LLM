from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([3, 24, 31]) & F.col("email").like("%@gmail.com"))
    .groupBy("is_remote", "level")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0))) / F.count("*")), 1).alias("pct_director"),
        F.count("*").alias("row_count"),
        F.max("salary").alias("max_salary"),
    )
)
