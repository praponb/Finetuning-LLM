from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("dept_id").isin([5, 23, 31]))
    .withColumn("clean_job_title", F.trim("job_title"))
)

level_summary = (
    base
    .groupBy("level", "is_remote")
    .agg(
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    level_summary
    .filter(F.col("min_salary") > 118000)
    .select("level", "is_remote", "l3_salary", "min_salary")
    .orderBy(F.col("level").desc())
)
