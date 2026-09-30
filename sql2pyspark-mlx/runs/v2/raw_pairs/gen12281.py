from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(~F.col("is_remote"))
    .withColumn("level_full_name_key", F.concat_ws(",", "level", "full_name"))
)

summary = (
    filtered_employees
    .groupBy("job_title")
    .agg(
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = summary.orderBy(F.col("salary_range").desc())
