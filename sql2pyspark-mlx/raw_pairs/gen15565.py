from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("avg_salary") < 30)
    .select("hire_month", "l2_count", "avg_salary")
    .orderBy(F.col("l2_count").desc())
)
