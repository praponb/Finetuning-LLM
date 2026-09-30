from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("job_title") == "Analyst") & F.col("is_remote"))
    .withColumn("salary_int", F.col("salary").cast("int"))
)

level_summary = (
    base
    .groupBy("level")
    .agg(F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"))
)

result = (
    level_summary
    .filter(F.col("l5_count") < 5)
    .select("level", "l5_count")
    .orderBy("l5_count")
)
