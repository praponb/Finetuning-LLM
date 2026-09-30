from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2013000)
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget")).otherwise(0)).alias("bangkok_budget"),
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
        F.count("*").alias("department_count"),
    )
    .filter(F.col("department_count") < 5)
    .drop("department_count")
)
