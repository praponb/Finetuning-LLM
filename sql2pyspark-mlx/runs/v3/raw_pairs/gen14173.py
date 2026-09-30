from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("full_name").like("%smith%") & (F.col("level") != "L4")) | (F.length("full_name") > 8))
    .groupBy("is_remote", F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
        F.sum("salary").alias("total_salary"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
    )
    .filter(F.col("l2_count") >= 5)
    .drop("l2_count")
)
