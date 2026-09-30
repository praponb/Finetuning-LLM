from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(~F.col("is_remote") & (F.col("salary") >= 91000))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
)

employees_stats = (
    filtered_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L5", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l5"),
    )
)

result = employees_stats.orderBy("stddev_salary")
