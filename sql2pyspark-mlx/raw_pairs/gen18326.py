from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "New York", "Bangkok"]) & F.col("dept_name").isin(["Finance", "Engineering", "Legal"]))
    .groupBy("location")
    .agg(F.count("budget").alias("budget_count"))
    .filter(F.col("budget_count") < 2)
    .orderBy("location")
    .limit(1)
)
