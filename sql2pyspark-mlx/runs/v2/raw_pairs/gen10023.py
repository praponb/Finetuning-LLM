from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "is_remote")
    .agg(
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .orderBy("manager_count")
)
