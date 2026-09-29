from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("dept_id").isin([5, 11]) & (F.col("level") != "L3"))
    .withColumn("full_name_filled", F.coalesce("full_name", F.lit("N/A")))
)

employees_stats = (
    src
    .groupBy("level", F.month("hire_date").alias("hire_month"))
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    employees_stats
    .orderBy("hire_month", "level")
    .limit(20)
)
