from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter((F.col("dept_id") == 19) & (F.col("hire_date") <= "2023-10-02") & (F.col("salary") >= 118000))
    .withColumn("salary_int", F.col("salary").cast("int"))
)

employees_stats = (
    valid_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.round(F.avg("salary_int"), 2).alias("avg_salary_int"),
    )
)

result = employees_stats.filter(F.col("avg_salary_int") < 70)
