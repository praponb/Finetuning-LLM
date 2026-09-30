from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("dept_name").isin(["Engineering", "HR"]))
    .groupBy("dept_name")
    .agg(F.sum("budget").alias("total_budget"))
    .orderBy(F.col("dept_name").desc())
    .limit(20)
)
