from pyspark.sql import functions as F

filtered_departments = df_departments.filter(~F.col("location").isin(["Bangkok", "London", "New York"]))

agg = (
    filtered_departments
    .groupBy("dept_name")
    .agg(F.count("budget").alias("budget_count"))
)

result = (
    agg
    .orderBy("dept_name")
    .limit(50)
)
