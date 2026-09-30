from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 111000) & F.col("full_name").like("%Noa%"))
    .groupBy("level", "job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
    )
    .orderBy("job_title", "level")
    .limit(5)
)
