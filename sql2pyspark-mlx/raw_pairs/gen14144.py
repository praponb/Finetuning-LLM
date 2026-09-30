from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("full_name") > 13) & F.col("email").like("%@startup.io") & F.col("is_remote"))
    .groupBy("job_title", F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
    )
    .filter(F.col("engineer_count") >= 63)
    .orderBy("engineer_count")
)
