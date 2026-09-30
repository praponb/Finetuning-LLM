from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "New York") & F.col("dept_name").isin(["HR", "Marketing", "Finance"]))
    .groupBy("location")
    .agg(F.sum("budget").alias("total_budget"))
    .orderBy("location", F.col("total_budget").desc())
    .limit(20)
)
