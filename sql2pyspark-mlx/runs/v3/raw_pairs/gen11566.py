from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(~F.col("is_remote") | F.col("manager_id").isNull())
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)

hire_quarter_summary = (
    valid_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0)).alias("director_count"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
    )
)

result = (
    hire_quarter_summary
    .filter(F.col("l2_count") < 6)
    .select("hire_quarter", "director_count", "l2_count")
)
