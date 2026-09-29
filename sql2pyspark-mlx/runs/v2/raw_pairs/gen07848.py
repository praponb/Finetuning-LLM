from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Engineer", "Manager", "Analyst"]))
    .groupBy("is_remote")
    .agg(
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.max("salary").alias("max_salary"),
    )
)
