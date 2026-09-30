from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("dept_id").isin([22, 27, 28, 36]) & F.col("dept_id").isin([18, 19])) | (F.col("dept_id") == 12))
    .withColumn("full_name_length", F.length("full_name"))
)

hire_month_summary = (
    base
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"))
)

result = (
    hire_month_summary
    .filter(F.col("l4_count") < 22)
    .select("hire_month", "l4_count")
    .orderBy("hire_month")
)
