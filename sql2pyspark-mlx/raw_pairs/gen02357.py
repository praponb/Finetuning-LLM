from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(~(F.col("full_name").isNotNull() & F.col("is_remote") & F.col("email").isNotNull()))
)

employees_stats = (
    valid_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = employees_stats.filter(F.col("avg_salary") < 34)
