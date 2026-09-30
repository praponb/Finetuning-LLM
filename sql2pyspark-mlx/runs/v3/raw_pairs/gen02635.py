from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([6, 16, 21]))
    .groupBy(F.quarter("hire_date").alias("hire_quarter"), "is_remote")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
    )
    .filter(F.col("manager_count") >= 39)
    .select("hire_quarter", "is_remote", "cnt", "manager_count")
    .orderBy("cnt")
)
