from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.col("manager_id").isNull() & F.col("is_remote") & (F.col("salary") > 83000))
)

agg = (
    filtered_employees
    .groupBy("job_title")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
    )
)

result = agg.filter(F.col("l2_count") >= 44)
